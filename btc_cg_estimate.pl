#!/usr/bin/env perl
use strict;
use warnings;
use utf8;

use DBI;
use JSON::PP qw(decode_json);
use LWP::UserAgent;
use Text::CSV_XS;
use Time::Piece;
use Time::Seconds qw(ONE_DAY);
use URI::Escape qw(uri_escape);

# ------------------------------------------------------------------------------
# btc_cg_estimate.pl
#
# Features:
#   - Reads one or more BTC transaction CSV files
#   - Merges them into a single transaction stream
#   - Deduplicates rows using oc_transaction_hash when available
#   - Uses Canadian pooled ACB method
#   - Bulk-prefills one BTC/CAD daily rate via CoinAPI historical timeseries
#   - Stores rates in SQLite cache
#   - Retries with backoff on 429 / 5xx
#   - Writes:
#       * capital_gains_detailed.csv
#       * schedule3_crypto.csv
#       * schedule3_crypto.txt
#
# Assumptions:
#   - positive BTC amount => acquisition
#   - negative BTC amount => disposition
#   - all transactions are against CAD
#   - timestamps are UTC
#   - sell-side network fees are outlays/expenses
#
# Usage:
#   export COINAPI_KEY='your_key_here'
#   perl btc_cg_estimate.pl transactions1.csv transactions2.csv [...]
#
# Optional env:
#   export CG_DB=btc_rates.sqlite
#   export DETAIL_CSV=capital_gains_detailed.csv
#   export SCHEDULE3_CSV=schedule3_crypto.csv
#   export SCHEDULE3_TXT=schedule3_crypto.txt
# ------------------------------------------------------------------------------

my @CSV_FILES = @ARGV;
die "Usage: $0 file1.csv [file2.csv ...]\n" unless @CSV_FILES;

my $API_KEY = $ENV{COINAPI_KEY}
    or die "Please set COINAPI_KEY in the environment.\n";

my $DB_FILE       = $ENV{CG_DB}         || 'btc_rates.sqlite';
my $DETAIL_CSV    = $ENV{DETAIL_CSV}    || 'capital_gains_detailed.csv';
my $SCHEDULE3_CSV = $ENV{SCHEDULE3_CSV} || 'schedule3_crypto.csv';
my $SCHEDULE3_TXT = $ENV{SCHEDULE3_TXT} || 'schedule3_crypto.txt';

my $COINAPI_BASE = 'https://api-historical.exrates.coinapi.io/v1';
my $BASE_ASSET   = 'BTC';
my $QUOTE_ASSET  = 'CAD';

my $MAX_RETRIES       = 6;
my $INITIAL_BACKOFF_S = 1;
my $MAX_BACKOFF_S     = 60;

# ------------------------------------------------------------------------------
# DB SETUP
# ------------------------------------------------------------------------------

my $dbh = DBI->connect("dbi:SQLite:dbname=$DB_FILE", "", "", {
    RaiseError => 1,
    AutoCommit => 1,
});

init_db($dbh);

# ------------------------------------------------------------------------------
# HTTP CLIENT
# ------------------------------------------------------------------------------

my $ua = LWP::UserAgent->new(
    timeout => 30,
    agent   => 'btc-cad-acb-coinapi-bulk/4.0',
);
$ua->env_proxy;

# ------------------------------------------------------------------------------
# CSV INPUT
# ------------------------------------------------------------------------------

my @txs;
my %needed_days;
my %seen_keys;

for my $csv_file (@CSV_FILES) {
    read_input_file($csv_file, \@txs, \%needed_days, \%seen_keys);
}

@txs = sort { $a->{epoch} <=> $b->{epoch} } @txs;
die "No non-zero BTC rows found in input files\n" unless @txs;

# ------------------------------------------------------------------------------
# BULK PREFILL DAILY RATES
# ------------------------------------------------------------------------------

prefill_daily_prices_bulk($dbh, $ua, $API_KEY, [ sort { $a <=> $b } keys %needed_days ]);

# ------------------------------------------------------------------------------
# ACB PROCESSING
# ------------------------------------------------------------------------------

my $holdings_btc      = 0.0;
my $acb_cad_total     = 0.0;
my $realized_gain_cad = 0.0;

my @detail_rows;
my %schedule3_by_year;

for my $tx (@txs) {
    my $price_cad = get_cached_daily_price($dbh, $tx->{day_bucket});
    my $fee_cad   = $tx->{fee_btc} * $price_cad;
    my $year      = gmtime($tx->{epoch})->year;

    if ($tx->{btc} > 0) {
        my $btc_bought = $tx->{btc};
        my $cad_cost   = ($btc_bought * $price_cad) + $fee_cad;

        $holdings_btc  += $btc_bought;
        $acb_cad_total += $cad_cost;

        push @detail_rows, {
            source_file       => $tx->{source_file},
            timestamp         => $tx->{timestamp_str},
            tax_year          => $year,
            type              => 'BUY',
            label             => $tx->{label},
            tx_hash           => $tx->{oc_transaction_hash},
            btc               => $btc_bought,
            price_cad         => $price_cad,
            fee_btc           => $tx->{fee_btc},
            fee_cad           => $fee_cad,
            proceeds_cad      => 0,
            acb_reduced_cad   => 0,
            outlays_cad       => $fee_cad,
            gain_cad          => 0,
            holdings_btc      => $holdings_btc,
            acb_total_cad     => $acb_cad_total,
            acb_per_btc_cad   => ($holdings_btc > 0 ? $acb_cad_total / $holdings_btc : 0),
            notes             => 'Acquisition increases pooled ACB',
        };
    }
    else {
        my $btc_sold = abs($tx->{btc});

        if ($btc_sold - $holdings_btc > 1e-12) {
            die sprintf(
                "Disposition exceeds holdings at %s: trying to sell %.8f BTC with only %.8f BTC held\n",
                $tx->{timestamp_str}, $btc_sold, $holdings_btc
            );
        }

        my $gross_proceeds      = $btc_sold * $price_cad;
        my $acb_per_btc         = ($holdings_btc > 0) ? ($acb_cad_total / $holdings_btc) : 0;
        my $cost_of_disposition = $btc_sold * $acb_per_btc;
        my $outlays_cad         = $fee_cad;
        my $gain_cad            = $gross_proceeds - $cost_of_disposition - $outlays_cad;

        $holdings_btc      -= $btc_sold;
        $acb_cad_total     -= $cost_of_disposition;
        $realized_gain_cad += $gain_cad;

        $holdings_btc  = 0.0 if abs($holdings_btc) < 1e-12;
        $acb_cad_total = 0.0 if abs($acb_cad_total) < 1e-8;

        push @detail_rows, {
            source_file       => $tx->{source_file},
            timestamp         => $tx->{timestamp_str},
            tax_year          => $year,
            type              => 'SELL',
            label             => $tx->{label},
            tx_hash           => $tx->{oc_transaction_hash},
            btc               => -$btc_sold,
            price_cad         => $price_cad,
            fee_btc           => $tx->{fee_btc},
            fee_cad           => $fee_cad,
            proceeds_cad      => $gross_proceeds,
            acb_reduced_cad   => $cost_of_disposition,
            outlays_cad       => $outlays_cad,
            gain_cad          => $gain_cad,
            holdings_btc      => $holdings_btc,
            acb_total_cad     => $acb_cad_total,
            acb_per_btc_cad   => ($holdings_btc > 0 ? $acb_cad_total / $holdings_btc : 0),
            notes             => 'Disposition for Schedule 3 style reporting',
        };

        $schedule3_by_year{$year}{proceeds_cad} += $gross_proceeds;
        $schedule3_by_year{$year}{acb_cad}      += $cost_of_disposition;
        $schedule3_by_year{$year}{outlays_cad}  += $outlays_cad;
        $schedule3_by_year{$year}{gain_cad}     += $gain_cad;
        $schedule3_by_year{$year}{btc_disposed} += $btc_sold;
        $schedule3_by_year{$year}{count}        += 1;
    }
}

write_detail_csv($DETAIL_CSV, \@detail_rows);
write_schedule3_csv($SCHEDULE3_CSV, \%schedule3_by_year);
write_schedule3_txt($SCHEDULE3_TXT, \%schedule3_by_year, $realized_gain_cad, $holdings_btc, $acb_cad_total);

print_summary($realized_gain_cad, $holdings_btc, $acb_cad_total, scalar(@txs));

# ------------------------------------------------------------------------------
# INPUT READER / MERGE
# ------------------------------------------------------------------------------

sub read_input_file {
    my ($csv_file, $txs_ref, $needed_days_ref, $seen_ref) = @_;

    my $csv_in = Text::CSV_XS->new({
        binary    => 1,
        auto_diag => 2,
    });

    open my $fh, '<:encoding(utf8)', $csv_file
        or die "Cannot open $csv_file: $!\n";

    my $header = $csv_in->getline($fh)
        or die "CSV appears to be empty: $csv_file\n";

    my %idx;
    @idx{@$header} = (0 .. $#$header);

    for my $required (qw(amount_chain_bc timestamp network_fee_satoshi label)) {
        die "Missing required CSV column '$required' in $csv_file\n"
            unless exists $idx{$required};
    }

    while (my $row = $csv_in->getline($fh)) {
        my $btc = parse_num($row->[$idx{amount_chain_bc}]);
        next if abs($btc) < 1e-16;

        my $ts_str = $row->[$idx{timestamp}];
        my $epoch  = parse_timestamp_utc($ts_str);
        my $day_bucket = day_bucket_utc($epoch);

        my $fee_sat = parse_num($row->[$idx{network_fee_satoshi}]);
        my $fee_btc = $fee_sat / 100_000_000.0;
        my $label   = $row->[$idx{label}] // '';

        my $oc_hash = exists $idx{oc_transaction_hash}
            ? ($row->[$idx{oc_transaction_hash}] // '')
            : '';

        my $ln_hash = exists $idx{ln_payment_hash}
            ? ($row->[$idx{ln_payment_hash}] // '')
            : '';

        my $dedupe_key;
        if (defined($oc_hash) && $oc_hash ne '') {
            $dedupe_key = "oc:$oc_hash";
        } else {
            $dedupe_key = join "\x1E",
                'fallback',
                $ts_str // '',
                $label  // '',
                sprintf('%.16f', $btc),
                sprintf('%.16f', $fee_sat),
                $ln_hash // '';
        }

        next if $seen_ref->{$dedupe_key}++;

        push @$txs_ref, {
            source_file          => $csv_file,
            timestamp_str        => $ts_str,
            epoch                => $epoch,
            day_bucket           => $day_bucket,
            label                => $label,
            btc                  => $btc,
            fee_sat              => $fee_sat,
            fee_btc              => $fee_btc,
            oc_transaction_hash  => $oc_hash,
            ln_payment_hash      => $ln_hash,
        };

        $needed_days_ref->{$day_bucket} = 1;
    }

    close $fh;
}

# ------------------------------------------------------------------------------
# DB
# ------------------------------------------------------------------------------

sub init_db {
    my ($dbh) = @_;

    $dbh->do(q{
        CREATE TABLE IF NOT EXISTS price_cache (
            asset_id_base TEXT NOT NULL,
            asset_id_quote TEXT NOT NULL,
            bucket_start INTEGER NOT NULL,
            price REAL NOT NULL,
            source_time TEXT,
            fetched_at INTEGER NOT NULL,
            PRIMARY KEY (asset_id_base, asset_id_quote, bucket_start)
        )
    });

    $dbh->do(q{
        CREATE INDEX IF NOT EXISTS idx_price_cache_lookup
        ON price_cache (asset_id_base, asset_id_quote, bucket_start)
    });
}

# ------------------------------------------------------------------------------
# BULK PREFILL
# ------------------------------------------------------------------------------

sub prefill_daily_prices_bulk {
    my ($dbh, $ua, $api_key, $needed_days) = @_;
    return unless @$needed_days;

    my %missing;
    for my $day (@$needed_days) {
        my ($exists) = $dbh->selectrow_array(
            q{
                SELECT 1
                FROM price_cache
                WHERE asset_id_base = ? AND asset_id_quote = ? AND bucket_start = ?
            },
            undef,
            $BASE_ASSET, $QUOTE_ASSET, $day
        );
        $missing{$day} = 1 unless $exists;
    }

    my @missing_days = sort { $a <=> $b } keys %missing;
    return unless @missing_days;

    my @ranges = coalesce_day_ranges(@missing_days);

    for my $range (@ranges) {
        my ($start_day, $end_day) = @$range;

        my $time_start = iso8601_millis_z($start_day);
        my $time_end   = iso8601_millis_z($end_day + ONE_DAY);

        my $url = sprintf(
            '%s/exchangerate/%s/%s/history?period_id=%s&time_start=%s&time_end=%s&limit=%d',
            $COINAPI_BASE,
            uri_escape($BASE_ASSET),
            uri_escape($QUOTE_ASSET),
            uri_escape('1DAY'),
            uri_escape($time_start),
            uri_escape($time_end),
            100000,
        );

        my $rows = fetch_coinapi_timeseries_with_retry($ua, $api_key, $url);
        store_timeseries_rows($dbh, $rows);
    }

    for my $day (@missing_days) {
        my ($exists) = $dbh->selectrow_array(
            q{
                SELECT 1
                FROM price_cache
                WHERE asset_id_base = ? AND asset_id_quote = ? AND bucket_start = ?
            },
            undef,
            $BASE_ASSET, $QUOTE_ASSET, $day
        );

        next if $exists;

        my $price = fetch_single_daily_rate_with_retry($ua, $api_key, $day);
        $dbh->do(
            q{
                INSERT OR REPLACE INTO price_cache
                (asset_id_base, asset_id_quote, bucket_start, price, source_time, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
            },
            undef,
            $BASE_ASSET, $QUOTE_ASSET, $day, $price, iso8601_millis_z($day), time()
        );
    }
}

sub coalesce_day_ranges {
    my @days = @_;
    return unless @days;

    my @ranges;
    my $start = $days[0];
    my $prev  = $days[0];

    for my $i (1 .. $#days) {
        my $day = $days[$i];
        if ($day == $prev + ONE_DAY) {
            $prev = $day;
            next;
        }
        push @ranges, [$start, $prev];
        $start = $day;
        $prev  = $day;
    }

    push @ranges, [$start, $prev];
    return @ranges;
}

sub store_timeseries_rows {
    my ($dbh, $rows) = @_;

    my $ins = $dbh->prepare(q{
        INSERT OR REPLACE INTO price_cache
        (asset_id_base, asset_id_quote, bucket_start, price, source_time, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?)
    });

    for my $r (@$rows) {
        next unless ref($r) eq 'HASH';

        my $period_start = $r->{time_period_start} // next;
        my $price =
               $r->{rate_close}
            // $r->{rate_open}
            // $r->{rate_high}
            // $r->{rate_low};

        next unless defined $price;

        my $bucket = parse_iso8601_z($period_start);
        $bucket = day_bucket_utc($bucket);

        $ins->execute(
            $BASE_ASSET,
            $QUOTE_ASSET,
            $bucket,
            $price,
            $period_start,
            time(),
        );
    }
}

sub get_cached_daily_price {
    my ($dbh, $day_bucket) = @_;

    my ($price) = $dbh->selectrow_array(
        q{
            SELECT price
            FROM price_cache
            WHERE asset_id_base = ? AND asset_id_quote = ? AND bucket_start = ?
        },
        undef,
        $BASE_ASSET, $QUOTE_ASSET, $day_bucket
    );

    die "Missing cached daily price for day bucket $day_bucket\n"
        unless defined $price;

    return $price;
}

# ------------------------------------------------------------------------------
# HTTP / RETRY
# ------------------------------------------------------------------------------

sub fetch_coinapi_timeseries_with_retry {
    my ($ua, $api_key, $url) = @_;

    my $attempt = 0;
    my $backoff = $INITIAL_BACKOFF_S;

    while (1) {
        $attempt++;

        my $req = HTTP::Request->new(GET => $url);
        $req->header('Accept'        => 'application/json');
        $req->header('X-CoinAPI-Key' => $api_key);

        my $res = $ua->request($req);

        if ($res->is_success) {
            my $data = eval { decode_json($res->decoded_content) };
            die "Failed to parse CoinAPI timeseries JSON: $@\n" if $@;
            die "CoinAPI timeseries response was not an array\n" unless ref($data) eq 'ARRAY';
            return $data;
        }

        retry_or_die($res, $attempt, \$backoff, 'CoinAPI timeseries request failed');
    }
}

sub fetch_single_daily_rate_with_retry {
    my ($ua, $api_key, $day_bucket) = @_;

    my $attempt = 0;
    my $backoff = $INITIAL_BACKOFF_S;
    my $time    = iso8601_millis_z($day_bucket);

    my $url = sprintf(
        '%s/exchangerate/%s/%s?time=%s',
        $COINAPI_BASE,
        uri_escape($BASE_ASSET),
        uri_escape($QUOTE_ASSET),
        uri_escape($time),
    );

    while (1) {
        $attempt++;

        my $req = HTTP::Request->new(GET => $url);
        $req->header('Accept'        => 'application/json');
        $req->header('X-CoinAPI-Key' => $api_key);

        my $res = $ua->request($req);

        if ($res->is_success) {
            my $data = eval { decode_json($res->decoded_content) };
            die "Failed to parse CoinAPI JSON: $@\n" if $@;

            my $price = $data->{rate};
            die "No rate returned from CoinAPI\n" unless defined $price;
            return $price;
        }

        retry_or_die($res, $attempt, \$backoff, 'CoinAPI specific-rate fallback failed');
    }
}

sub retry_or_die {
    my ($res, $attempt, $backoff_ref, $prefix) = @_;

    my $code = $res->code || 0;
    my $retryable = ($code == 429 || ($code >= 500 && $code <= 599)) ? 1 : 0;

    if ($retryable && $attempt < $MAX_RETRIES) {
        my $retry_after = $res->header('Retry-After');
        my $sleep_for;

        if (defined $retry_after && $retry_after =~ /^\d+(?:\.\d+)?$/) {
            $sleep_for = $retry_after;
        } else {
            $sleep_for = $$backoff_ref;
        }

        select(undef, undef, undef, $sleep_for);

        $$backoff_ref *= 2;
        $$backoff_ref = $MAX_BACKOFF_S if $$backoff_ref > $MAX_BACKOFF_S;
        return;
    }

    die $prefix . ': ' . $res->status_line . "\n" . $res->decoded_content . "\n";
}

# ------------------------------------------------------------------------------
# REPORT WRITERS
# ------------------------------------------------------------------------------

sub write_detail_csv {
    my ($filename, $rows) = @_;

    my $csv = Text::CSV_XS->new({ binary => 1, eol => "\n" });
    open my $out, '>:encoding(utf8)', $filename
        or die "Cannot write $filename: $!\n";

    $csv->print($out, [
        qw(
            source_file tx_hash timestamp tax_year type label btc price_cad fee_btc fee_cad
            proceeds_cad acb_reduced_cad outlays_cad gain_cad
            holdings_btc acb_total_cad acb_per_btc_cad notes
        )
    ]);

    for my $r (@$rows) {
        $csv->print($out, [
            $r->{source_file},
            $r->{tx_hash},
            $r->{timestamp},
            $r->{tax_year},
            $r->{type},
            $r->{label},
            fmt8($r->{btc}),
            fmt8($r->{price_cad}),
            fmt8($r->{fee_btc}),
            fmt2($r->{fee_cad}),
            fmt2($r->{proceeds_cad}),
            fmt2($r->{acb_reduced_cad}),
            fmt2($r->{outlays_cad}),
            fmt2($r->{gain_cad}),
            fmt8($r->{holdings_btc}),
            fmt2($r->{acb_total_cad}),
            fmt2($r->{acb_per_btc_cad}),
            $r->{notes},
        ]);
    }

    close $out;
}

sub write_schedule3_csv {
    my ($filename, $by_year) = @_;

    my $csv = Text::CSV_XS->new({ binary => 1, eol => "\n" });
    open my $out, '>:encoding(utf8)', $filename
        or die "Cannot write $filename: $!\n";

    $csv->print($out, [
        qw(
            tax_year schedule3_section property_type acquisitions_year
            dispositions_count total_btc_disposed proceeds_of_disposition_cad
            adjusted_cost_base_cad outlays_and_expenses_cad gain_or_loss_cad
            taxable_capital_gain_estimate_cad
        )
    ]);

    for my $year (sort { $a <=> $b } keys %$by_year) {
        my $r = $by_year->{$year};

        $csv->print($out, [
            $year,
            'Part 3',
            'Crypto-assets (line 7 style)',
            'Various',
            $r->{count} || 0,
            fmt8($r->{btc_disposed} || 0),
            fmt2($r->{proceeds_cad} || 0),
            fmt2($r->{acb_cad} || 0),
            fmt2($r->{outlays_cad} || 0),
            fmt2($r->{gain_cad} || 0),
            fmt2(($r->{gain_cad} || 0) * 0.50),
        ]);
    }

    close $out;
}

sub write_schedule3_txt {
    my ($filename, $by_year, $realized_gain_cad, $holdings_btc, $acb_cad_total) = @_;

    open my $out, '>:encoding(utf8)', $filename
        or die "Cannot write $filename: $!\n";

    print {$out} "Schedule 3 style crypto-assets summary\n";
    print {$out} "======================================\n\n";

    for my $year (sort { $a <=> $b } keys %$by_year) {
        my $r = $by_year->{$year};

        print {$out} "Tax year: $year\n";
        print {$out} "Property type: Crypto-assets (line 7 style)\n";
        print {$out} "Year of acquisition: Various\n";
        print {$out} "Number of dispositions: " . ($r->{count} || 0) . "\n";
        print {$out} "BTC disposed: " . fmt8($r->{btc_disposed} || 0) . "\n";
        print {$out} "Proceeds of disposition (CAD): " . fmt2($r->{proceeds_cad} || 0) . "\n";
        print {$out} "Adjusted cost base (CAD): " . fmt2($r->{acb_cad} || 0) . "\n";
        print {$out} "Outlays and expenses (CAD): " . fmt2($r->{outlays_cad} || 0) . "\n";
        print {$out} "Capital gain/loss (CAD): " . fmt2($r->{gain_cad} || 0) . "\n";
        print {$out} "Taxable capital gain estimate 50% (CAD): " . fmt2(($r->{gain_cad} || 0) * 0.50) . "\n\n";
    }

    print {$out} "Overall summary\n";
    print {$out} "---------------\n";
    print {$out} "Realized capital gain/loss (CAD): " . fmt2($realized_gain_cad) . "\n";
    print {$out} "Taxable capital gain estimate 50% (CAD): " . fmt2($realized_gain_cad * 0.50) . "\n";
    print {$out} "Ending BTC holdings: " . fmt8($holdings_btc) . "\n";
    print {$out} "Ending pooled ACB total (CAD): " . fmt2($acb_cad_total) . "\n";
    print {$out} "Ending pooled ACB per BTC (CAD): " .
        fmt2($holdings_btc > 0 ? $acb_cad_total / $holdings_btc : 0) . "\n";

    close $out;
}

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

sub parse_num {
    my ($v) = @_;
    return 0.0 if !defined($v) || $v eq '';
    $v =~ s/^\s+//;
    $v =~ s/\s+$//;
    $v =~ s/,$//;
    return 0.0 + $v;
}

sub parse_timestamp_utc {
    my ($s) = @_;
    my $tp = Time::Piece->strptime($s, '%Y-%m-%d %H:%M:%S');
    return $tp->epoch;
}

sub parse_iso8601_z {
    my ($s) = @_;
    $s =~ s/\.\d+Z$/Z/;
    my $tp = Time::Piece->strptime($s, '%Y-%m-%dT%H:%M:%SZ');
    return $tp->epoch;
}

sub day_bucket_utc {
    my ($epoch) = @_;
    return int($epoch / 86400) * 86400;
}

sub iso8601_millis_z {
    my ($epoch) = @_;
    my $tp = gmtime($epoch);
    return $tp->strftime('%Y-%m-%dT%H:%M:%S') . '.000Z';
}

sub fmt2 {
    my ($n) = @_;
    $n ||= 0;
    return sprintf('%.2f', $n);
}

sub fmt8 {
    my ($n) = @_;
    $n ||= 0;
    return sprintf('%.8f', $n);
}

sub print_summary {
    my ($realized_gain_cad, $holdings_btc, $acb_cad_total, $tx_count) = @_;

    print "Summary\n";
    print "-------\n";
    printf "Merged transactions processed: %d\n", $tx_count;
    printf "Realized capital gain/loss (CAD): %s\n", fmt2($realized_gain_cad);
    printf "Taxable capital gain estimate (50%%, CAD): %s\n", fmt2($realized_gain_cad * 0.50);
    printf "Ending BTC holdings: %s\n", fmt8($holdings_btc);
    printf "Ending ACB total (CAD): %s\n", fmt2($acb_cad_total);
    printf "Ending ACB per BTC (CAD): %s\n",
        fmt2($holdings_btc > 0 ? $acb_cad_total / $holdings_btc : 0);

    print "\nFiles written\n";
    print "-------------\n";
    print "$DETAIL_CSV\n";
    print "$SCHEDULE3_CSV\n";
    print "$SCHEDULE3_TXT\n";
}
