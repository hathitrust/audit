#!/usr/bin/perl

use strict;
use warnings;
use FindBin;
use lib $FindBin::Bin;
use HTFeed::DBTools qw(get_dbh);
use HTFeed::Volume;
use HTFeed::VolumeValidator;
use HTFeed::METS;
use HTFeed::Log;
use URI::Escape;
use DataDenPath;
use Cwd;


my $insert_detail =
"insert into feed_audit_detail (namespace, id, path, status, detail) values (?,?,?,?,?)";

my $zipfile = shift @ARGV or die("Missing zip file..");
$zipfile = Cwd::abs_path($zipfile);
die("zip file $zipfile does not exist") unless -f $zipfile;
my $pathinfo = DataDenPath->new($zipfile);
check_zip($pathinfo);


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

    my $zipname = $pathinfo->zipinfo->{zip_file};

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
          $pathinfo->zipinfo->{zip_path},
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


__END__
