#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use HTFeed::Config qw(get_config);
use HTFeed::DBTools qw(get_dbh);
use File::Basename;
use File::Pairtree qw(ppath2id s2ppchars);
use HTFeed::Volume;
use HTFeed::VolumeValidator;
use HTFeed::Namespace;
use HTFeed::PackageType;
use HTFeed::METS;
use POSIX qw(strftime);
use URI::Escape;

my $tombstone_check = "select is_tombstoned from feed_audit where namespace = ? and id = ?";

my $insert =
"insert into feed_audit (namespace, id, sdr_partition, zip_size, zip_date, mets_size, mets_date, lastchecked) values(?,?,?,?,?,?,?,CURRENT_TIMESTAMP) \
ON DUPLICATE KEY UPDATE sdr_partition = ?, zip_size=?, zip_date =?,mets_size=?,mets_date=?,lastchecked = CURRENT_TIMESTAMP";

my $update =
"update feed_audit set lastchecked = CURRENT_TIMESTAMP where namespace = ? and id = ? and version = ?";

my $insert_detail =
"insert into feed_audit_detail (namespace, id, path, status, detail) values (?,?,?,?,?)";

### set /sdr1 to /sdrX for test & parallelization
my $prevpath;

my $base = shift @ARGV or die("Missing base directory..");

my ($sdr_partition) = ($base =~ qr#/?sdr(\d+)/?#);

open( RUN, "find $base -follow -type f|" )
  or die("Can't open pipe to find: $!");

while ( my $line = <RUN> ) {
  chomp($line);

  my @newList = ();    #initialize array
  next if $line =~ /\Qpre_uplift.mets.xml\E/;
  # ignore temporary location
  next if $line =~ qr(obj/\.tmp);

  eval {
    my $pathinfo = DataDenPath->new($line);

    # Don't process the same directory twice
    return if ($prevpath and $pathinfo->{path} eq $prevpath);
    $prevpath = $pathinfo->{path};


    check_pairtree($pathinfo);

    check_files($pathinfo);


    # TODO: verify against inventory - zipsize/metssize should match. if not, error & do not update
    # TODO: if OK, insert and warn if missing
    # TODO: update lastchecked if everything is OK
    # TODO: warn if missing in inventory and incomplete in data den

    # does barcode have a zip & xml, and do they match?


  };

  if ($@) {
    warn($@);
  }
}

sub set_status {
  warn( join( " ", @_ ), "\n" );
  execute_stmt( $insert_detail, @_ );
}

sub execute_stmt {
  my $stmt = shift;
  my $dbh  = get_dbh();
  my $sth  = $dbh->prepare($stmt);
  $sth->execute(@_);
  return $sth;
}

sub check_pairtree {
  my $pathinfo = shift;

  my @pt_components = ( s2ppchars($pathinfo->{pt_path_objid}), $pathinfo->{pt_terminal} );

  foreach my $should_be_pt_objid ( @pt_components ) {
    if ( $pathinfo->{pt_objid} ne $should_be_pt_objid) {
      set_status( $namespace, $pathinfo->{pt_path_objid}, $path, "BAD_PAIRTREE",
        "$should_be_pt_objid $pathinfo->{pt_objid}" );
    }
  }
}

sub check_files {
  my $pathinfo = shift;

  opendir( my $dh, $pathinfo->{path} );
  my $filecount  = 0;
  my $found_zip  = 0;
  my $found_mets = 0;
  while ( my $file = readdir($dh) ) {
    next if $file eq '.' or $file eq '..';

    if ( $file !~ /^([^.]+)\.(zip|mets.xml)$/ ) {
      print("BAD_FILE $path $file\n");
    }

    my $dir_barcode = $1;
    my $ext         = $2;
    $found_zip++  if $ext eq 'zip';
    $found_mets++ if $ext eq 'mets.xml';

    if ( $pathinfo->{pt_objid} ne $dir_barcode ) {
      set_status( $namespace, $pathinfo->{pt_path_objid}, $path, "BARCODE_MISMATCH",
        "$pathinfo->{pt_objid} $dir_barcode" );
    }
    $filecount++;
  }

  if ( $filecount > 2 or $filecount < 1 or $found_zip != 1 or $found_mets != 1 ) {
    set_status( $namespace, $objid, $path, "BAD_FILECOUNT",
      "zip=$found_zip mets=$found_mets total=$filecount" );
  }

  closedir($dh);
}

get_dbh()->disconnect();
close(RUN);

__END__
