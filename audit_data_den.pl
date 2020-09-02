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



my $insert =
'insert into feed_backups (namespace, id, version, zip_size, mets_size, lastchecked)'.
' values(?,?,?,?,?,CURRENT_TIMESTAMP)';

my $update_lastchecked =
"update feed_backups set lastchecked = CURRENT_TIMESTAMP where namespace = ? and id = ? and version = ?";

my $insert_detail =
"insert into feed_audit_detail (namespace, id, path, status, detail) values (?,?,?,?,?)";

my $select_record =
"select zip_size, mets_size from feed_backups where namespace = ? and id = ? and version = ?";

my $select_max_lastchecked =
'select max(lastchecked) from feed_backups';

my $select_unchecked =
'select namespace, id, version from feed_backups where lastchecked <= ?';

my $base = shift @ARGV or die("Missing base directory..");
$base .= '/' unless substr($base, -1, 1) eq '/';

my $last_run = '0000-00-00 00:00:00';
my $sth = execute_stmt($select_max_lastchecked);
if (my @row = $sth->fetchrow_array()) {
  $last_run = $row[0];
}
$sth->finish();

open( RUN, "find $base -follow -type f|" )
  or die("Can't open pipe to find: $!");


my $prevpath;

while ( my $line = <RUN> ) {
  chomp($line);

  #next if $line =~ /\Qpre_uplift.mets.xml\E/;
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
    check_zip($pathinfo);

  };

  if ($@) {
    warn($@);
  }
}

$sth = execute_stmt($select_unchecked, $last_run);
while (my @row = $sth->fetchrow_array()) {
  my $namespace = $row[0];
  my $id = $row[1];
  my $version = $row[2];
  my $where = join('/', $base, 'obj', $namespace, id2ppath($id), $id, $version);
  $where =~ s,//,/,g;
  set_status( $namespace, $id, $where, "DATA_DEN_MISSING", '' );
}
$sth->finish();

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

sub check_zip {
  my $pathinfo = shift;
  my $do_md5 = 1;
  my $do_mets = 1;

  # use google as a 'default' namespace for now
  my $volume = new HTFeed::Volume(
    packagetype => 'pkgtype',
    namespace   => $pathinfo->{namespace},
    objid       => $pathinfo->{pt_path_objid}
  );

  my $mets = $volume->_parse_xpc($pathinfo->metsinfo->{mets_path});
  my $rval = undef;

# Extract the checksum for the zip file that looks kind of like this:
#  <METS:fileGrp ID="FG1" USE="zip archive">
#     <METS:file ID="ZIP00000001" MIMETYPE="application/zip" SEQ="00000001" CREATED="2008-11-22T20:07:28" SIZE="30844759" CHECKSUM="42417b735ae73a3e16d1cca59c7fac08" CHECKSUMTYPE="MD5">
#       <METS:FLocat LOCTYPE="OTHER" OTHERLOCTYPE="SYSTEM" xlink:href="39015603581748.zip" />
#     </METS:file>
#  </METS:fileGrp>

  if ($do_md5) {
    my $zipname = $pathinfo->{zipinfo}->{zip_file};

    my $mets_zipsum = $mets->findvalue(
      "//mets:file[mets:FLocat/\@xlink:href='$zipname']/\@CHECKSUM");

    if(not defined $mets_zipsum or length($mets_zipsum) ne 32) {
      # zip name may be uri-escaped in some cases
      $zipname = uri_escape($zipname);
      $mets_zipsum = $mets->findvalue(
        "//mets:file[mets:FLocat/\@xlink:href='$zipname']/\@CHECKSUM");
    }

    if ( not defined $mets_zipsum or length($mets_zipsum) ne 32 ) {
      set_status( $pathinfo->{namespace}, $pathinfo->{pt_path_objid}, $pathinfo->metsinfo->{mets_path},
        "MISSING_METS_CHECKSUM", (defined $mets_zipsum)? $mets_zipsum : '<undef>' );
    }
    else {
      my $realsum = HTFeed::VolumeValidator::md5sum(
        $pathinfo->zipinfo->{zip_path} );
      if ( $mets_zipsum eq $realsum ) {
        $rval = 1;
      }
      else {
        set_status( $pathinfo->{namespace}, $pathinfo->{pt_path_objid},
          $pathinfo->{zipinfo}->{zip_path},
          "BAD_CHECKSUM", "expected=$mets_zipsum actual=$realsum" );
        $rval = 0;
      }
    }
  }

  if ($do_mets) {

    {    # METS valid
      my ( $mets_valid, $error ) =
      HTFeed::METS::validate_xml( { volume => $volume },
        $pathinfo->metsinfo->{mets_path} );
      if ( !$mets_valid ) {
        $error =~ s/\n/ /mg;
        set_status( $pathinfo->{namespace}, $pathinfo->{pt_path_objid},
          $pathinfo->metsinfo->{mets_path}, "INVALID_METS", $error );
      }
    }
  }
  return $rval;
}



get_dbh()->disconnect();
close(RUN);

__END__
