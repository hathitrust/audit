#!/usr/bin/perl

use strict;
use warnings;
use FindBin;
use lib $FindBin::Bin;
use DBI;
use HTFeed::Config qw(get_config);
use HTFeed::DBTools qw(get_dbh);
use File::Basename;
use File::Pairtree qw(ppath2id s2ppchars id2ppath);
use HTFeed::Volume;
use HTFeed::VolumeValidator;
use HTFeed::Namespace;
use HTFeed::PackageType;
use HTFeed::METS;
use HTFeed::Log;
use POSIX qw(strftime);
use URI::Escape;
use DataDenPath;

my $check_missing = 1;

my $insert =
'insert into feed_backups (namespace, id, version, zip_size, mets_size, lastchecked)'.
' values(?,?,?,?,?,CURRENT_TIMESTAMP)';

my $update_lastchecked =
"update feed_backups set lastchecked = CURRENT_TIMESTAMP where namespace = ? and id = ? and version = ?";

my $insert_detail =
"insert into feed_audit_detail (namespace, id, path, status, detail) values (?,?,?,?,?)";

my $select_record =
"select zip_size, mets_size from feed_backups where namespace = ? and id = ? and version = ?";

my $select_now =
'select now()';

my $select_unchecked =
'select namespace, id, version from feed_backups where lastchecked < ?';

my $base = shift @ARGV or die("Missing base directory..");
$base .= '/' unless substr($base, -1, 1) eq '/';

my $start_time;
my $sth = execute_stmt($select_now);
if (my @row = $sth->fetchrow_array()) {
  $start_time = $row[0];
}
$sth->finish();
die "Failed to determine current time.." unless defined $start_time;

open( RUN, "find $base -follow -type f|" )
  or die("Can't open pipe to find: $!");


my $prevpath;

while ( my $line = <RUN> ) {
  chomp($line);

  # ignore temporary location
  next if $line =~ qr(obj/\.tmp);

  eval {
    my $pathinfo = DataDenPath->new($line);

    # Don't process the same directory twice
    return if ($prevpath and $pathinfo->{path} eq $prevpath);
    $prevpath = $pathinfo->{path};

    check_pairtree($pathinfo);
    check_files($pathinfo);
    check_record($pathinfo);

  };

  if ($@) {
    warn($@);
  }
}

check_missing_from_dataden($start_time) if $check_missing;

sub check_missing_from_dataden {
  my $start_time = shift;

  $sth = execute_stmt($select_unchecked, $start_time);
  while (my @row = $sth->fetchrow_array()) {
    my $namespace = $row[0];
    my $id = $row[1];
    my $version = $row[2];
    my $where = join('/', $base, 'obj', $namespace, id2ppath($id), $id, $version);
    $where =~ s,//,/,g;
    set_status( $namespace, $id, $where, "DATA_DEN_MISSING", '' );
  }
  $sth->finish();
}

sub set_status {
  #warn( join( " ", @_ ), "\n" );
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
      set_status( $pathinfo->{namespace}, $pathinfo->{pt_path_objid}, $pathinfo->{path}, "BAD_PAIRTREE",
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
    $filecount++;
    if ( $file !~ /^([^.]+)\.(zip|mets.xml)$/ ) {
      set_status( $pathinfo->{namespace}, $pathinfo->{pt_path_objid}, $pathinfo->{path}, "BAD_FILE",
        $file );
      next;
    }

    my $dir_barcode = $1;
    my $ext         = $2;
    $found_zip++  if $ext eq 'zip';
    $found_mets++ if $ext eq 'mets.xml';

    if ( $pathinfo->{pt_objid} ne $dir_barcode ) {
      set_status( $pathinfo->{namespace}, $pathinfo->{pt_path_objid}, $pathinfo->{path}, "BARCODE_MISMATCH",
        "$pathinfo->{pt_objid} $dir_barcode" );
    }
  }

  if ( $filecount > 2 or $filecount < 1 or $found_zip != 1 or $found_mets != 1 ) {
    set_status( $pathinfo->{namespace}, $pathinfo->{pt_path_objid}, $pathinfo->{path}, "BAD_FILECOUNT",
      "zip=$found_zip mets=$found_mets total=$filecount" );
  }

  closedir($dh);
}

sub check_record {
  my $pathinfo = shift;

  my $sth = execute_stmt($select_record, $pathinfo->{namespace},
                         $pathinfo->{pt_path_objid}, $pathinfo->{version});
  if (my @row = $sth->fetchrow_array()) {
    # Object found in DB
    my $zip_size = $row[0];
    my $mets_size = $row[1];
    if ($zip_size != $pathinfo->zipinfo->{zip_size}) {
      set_status( $pathinfo->{namespace}, $pathinfo->{pt_path_objid},
                  $pathinfo->{path}, 'DB_FILE_SIZE_MISMATCH',
                  "DB zipsize=$zip_size, actual zipsize=$pathinfo->zipinfo->{zip_size}");
    }
    elsif ($mets_size != $pathinfo->metsinfo->{mets_size}) {
      set_status( $pathinfo->{namespace}, $pathinfo->{pt_path_objid},
                  $pathinfo->{path}, 'DB_FILE_SIZE_MISMATCH',
                  "DB metssize=$mets_size, actual metssize=$pathinfo->metsinfo->{mets_size}");
    }
    else {
      # Update feed_backups.lastchecked
      execute_stmt($update_lastchecked, $pathinfo->{namespace},
                   $pathinfo->{pt_path_objid}, $pathinfo->{version});
    }
  }
  else {
    # Object missing from DB
    set_status( $pathinfo->{namespace}, $pathinfo->{pt_path_objid}, $pathinfo->{path}, "DB_RECORD_MISSING",
                "no record of $pathinfo->{namespace}.$pathinfo->{pt_path_objid} v.$pathinfo->{version} in feed_backups" );
    execute_stmt($insert, $pathinfo->{namespace}, $pathinfo->{pt_path_objid},
                 $pathinfo->{version}, $pathinfo->zipinfo->{zip_size},
                 $pathinfo->metsinfo->{mets_size});
  }
}



get_dbh()->disconnect();
close(RUN);

__END__
