#!/usr/bin/perl

use strict;
use warnings;
use POSIX qw(strftime);
use HTFeed::Config qw(get_config);
use Data::Dumper;
use Getopt::Long;
use Date::Parse;
# step 1: fetch hathifiles, extract IDs, sort | uniq.

# should we do each stage?
my $fetch_allids = 0;
my $fetch_hathifiles = 0;
my $do_audit = 1;

GetOptions(
    'fetch-all-ids|f!' => \$fetch_allids,
    'fetch-hathifiles|h!' => \$fetch_hathifiles,
    'do-audit|a!' => \$do_audit
);

# must be like 20110914 (YYYYMMDD)
my $audit_as_of = shift @ARGV;
# limit auditing to namespace
my $namespace = shift @ARGV;

my $last_has = "";
my $objid = "";

# command for sorting
my $SORT = "sort -S 4G -T /ram";

# handle server
my $datasource = get_config('handle'=>'database'=>'datasource');
my $user = get_config('handle'=>'database'=>'username');
my $passwd = get_config('handle'=>'database'=>'password');


die("Usage: $0 [--fetch-all-ids --fetch-hathifiles --do-audit] YYYYMMDD\n") unless $audit_as_of and $audit_as_of =~ /^\d{8}$/;

my $audit_epoch = str2time($audit_as_of);

if($fetch_hathifiles) {
    my @hathifiles = ();
    system("rm -v hathifiles_*_$audit_as_of");

    my $month = substr($audit_as_of,0,6);
    my $audit_day = substr($audit_as_of,6,2);
    push(@hathifiles,"hathi_full_${month}01.txt.gz");
    for (my $i = 1; $i < $audit_day; $i++) {
        my $day = sprintf("%02d",$i);
        push(@hathifiles,"hathi_upd_$month$day.txt.gz");
    }

    foreach my $hathifile (@hathifiles) {
        print "Getting $hathifile\n";
        if($namespace) {
            system("wget -q -O - http://www.hathitrust.org/sites/www.hathitrust.org/files/hathifiles/$hathifile | gzip -d | cut -f 1 | grep '^$namespace' >> hathifiles_ids_unsorted_$audit_as_of");
        } else {
            system("wget -q -O - http://www.hathitrust.org/sites/www.hathitrust.org/files/hathifiles/$hathifile | gzip -d | cut -f 1 >> hathifiles_ids_unsorted_$audit_as_of");
        }
    }

    print "Sorting hathifiles\n";
    system("$SORT -k 1,1 hathifiles_ids_unsorted_$audit_as_of | uniq > hathifiles_audit_$audit_as_of");
}


if( $fetch_allids ) {
    # step 2: generate list of all IDs. sources:
    #  hathifiles
    #  rights_log
    #  feed_audit
    #  feed_zephir_items
    #  feed_queue
    #  feed_queue_done
    #  handles

    # mdp/mdp_tracking tables


    my $tables = {
        'rights_log' => 'time',
        'feed_audit' => 'zip_date',
        'feed_zephir_items' => '1970-01-01',
        'feed_queue' => 'update_stamp',
        'feed_queue_done' => 'update_stamp',
    };

    while (my ($table,$time_col) = each(%$tables)) {
        print "fetching IDs from $table\n";
        if($namespace) {
            mysql_dump($table,"select concat(namespace,'.',id), $time_col from $table and namespace = '$namespace'");
        } else {
            mysql_dump($table,"select concat(namespace,'.',id), $time_col from $table");
        }
    }

    mysql_dump('nonreturned',"select concat(namespace,'.',id), '1970-01-01' from feed_zephir_items where returned = '0'");
    mysql_dump('returned',"select concat(namespace,'.',id), '1970-01-01' from feed_zephir_items where returned = '1'");

    print "Fetching handles\n";
    if($namespace) {
        mysql_dump_handles("select lower(substring(handle,6)), from_unixtime(timestamp) from handles where type = 'URL' and data like 'http://babel.hathitrust.org%' and handle like '2027/$namespace.%'");
    } else {
        mysql_dump_handles("select lower(substring(handle,6)), from_unixtime(timestamp) from handles where type = 'URL' and data like 'http://babel.hathitrust.org%'");
    }

    # sort | uniq
    print "sorting ids\n";
    system("cat *_audit_$audit_as_of | cut -f 1 | $SORT -k 1,1 | uniq > ht_all_ids_audit_$audit_as_of");
}

if($do_audit) {
    # step 3: comm hathifiles & all IDs. unexpected error if something is in hathifiles but not all IDs.
    open(my $allids_fh,"<ht_all_ids_audit_$audit_as_of") or die("Can't open ht_all_ids_audit_$audit_as_of");

    my $audit_fhs = {};
    my $audit_fh_buffers = {};
    # for each ID, fetch all info from tables above 
    foreach my $table (qw(rights_log feed_audit feed_zephir_items feed_queue feed_queue_done handle hathifiles)) {
        open($audit_fhs->{$table},"<${table}_audit_${audit_as_of}") or die("Can't open ${table}_audit_${audit_as_of}: $!");
    }

    my $count = 0;
    while($objid = <$allids_fh>) {

        $count++;
        print STDERR "." if($count % 10000 == 0);
        print STDERR "$count\n" if($count % 100000 == 0);
        chomp $objid;
        my ($namespace,$id) = split(/\./,$objid);
        my $vol_info = {};

        while(my ($table, $fh) = each(%$audit_fhs)) {
            while(not defined $audit_fh_buffers->{$table}
                  or $audit_fh_buffers->{$table}[0] lt $objid) {
              my $line = <$fh>;
              last if not defined $line;
              chomp($line);
              my @fields = split(/\t/,$line);
              $audit_fh_buffers->{$table} = [@fields];
            }
            if(defined $audit_fh_buffers->{$table} and 
                $audit_fh_buffers->{$table}[0] eq $objid) {
                $vol_info->{$table} = $audit_fh_buffers->{$table};
            }
        }

        # Audit rules for each ID:

        # hathifiles -> audit, rights_log, handle
        has($vol_info,'hathifiles') &&  audit_has($vol_info,[qw(feed_audit rights_log handle returned)]);
        # audit, age > 2 days -> rights_log, hathifiles, handle
        has($vol_info,'feed_audit',2) && audit_has($vol_info,[qw(rights_log hathifiles handle returned)]);
        # rights_log -> audit, hathifiles, handle (warning only)
        has($vol_info,'rights_log',0) && audit_has($vol_info,[qw(feed_audit hathifiles handle feed_zephir_items)]);
        # rights_log.source = google -> grin
        has($vol_info,'nonreturned') && audit_hasnt($vol_info,[qw(feed_audit hathifiles feed_queue feed_queue_done)]);
        # queue_done -> audit, rights_log, hathifiles
        has($vol_info,'feed_queue_done',0) && audit_has($vol_info,[qw(feed_audit rights_log hathifiles returned)]);
        # handle -> audit
        has($vol_info,'handle',0) && audit_has($vol_info,[qw(feed_audit)]);
    }
}

sub has {
    my $obj = shift;
    my $key = shift;
    my $days_before_audit = shift;
    $last_has = $key;
    if( defined $obj->{$key} and 
        (not defined $days_before_audit
                or (defined $obj->{$key}[1]
                    and $audit_epoch - str2time($obj->{$key}[1]) > 86400*$days_before_audit))) {
#        print "$objid has $key\n";
        return 1;
    } else {
        return 0;
    }
}

sub audit_has {
    my $obj = shift;
    my $keys = shift;
    my $error = shift;
    $error = 1  if not defined $error;
    my $errtxt = $error ? "ERROR" : "WARNING";

    foreach my $key (@$keys) {
        if(not defined $obj->{$key}) {
            print "$errtxt: $objid has $last_has but not $key\n";
        } else {
#            print "OK: $objid has $last_has and $key\n";
        }
    }
}

sub audit_hasnt {
    my $obj = shift;
    my $keys = shift;
    my $error = shift;
    $error = 1  if not defined $error;
    my $errtxt = $error ? "ERROR" : "WARNING";

    foreach my $key (@$keys) {
        if(defined $obj->{$key}) {
            print "$errtxt: $objid has both $last_has and $key\n";
        } else {
#            print "OK: $objid has $last_has and not $key\n";
        }
    }
}

sub mysql_dump {
    # babel
    my $datasource = get_config('database'=>'datasource');
    my $user = get_config('database'=>'username');
    my $passwd = get_config('database'=>'password');
    return mysql_dump_generic($datasource,$user,$passwd,@_);
}

sub mysql_dump_generic {
    my $dsn = shift;
    my $user = shift;
    my $password = shift;
    my $table = shift;
    my $query = shift;
    my ($proto,$driver,$db,$host) = split(":",$dsn);

    print STDERR $query, "\n";

    system(qq(mysql -B -N -h $host -u $user --password="$password" $db  -e "$query" | $SORT -k 1,1 > ${table}_audit_${audit_as_of}));
}

sub mysql_dump_handles {
    # handle server
    my $datasource = get_config('handle'=>'database'=>'datasource');
    my $user = get_config('handle'=>'database'=>'username');
    my $passwd = get_config('handle'=>'database'=>'password');

    return mysql_dump_generic($datasource,$user,$passwd,'handle',@_);
}
