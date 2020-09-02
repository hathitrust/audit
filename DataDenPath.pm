package DataDenPath;

use POSIX qw(strftime);
use File::Basename;
use File::Pairtree;

sub new {
  my $class = shift;
  my $filepath = shift;

  my $self = {};
  bless $self, $class;
  $self->fileparse($filepath);
  $self->pathcomps($filepath);
  return $self;

}

sub pathcomps {
  my $self = shift;
  my @pathcomp = split( "/", $self->{path});

  # remove base & any empty components
  @pathcomp = grep { $_ ne '' } @pathcomp;
my @basepath;
while (1) {
  my $comp = shift @pathcomp;
  push @basepath, $comp;
  last if $comp eq 'obj';
}

  $self->{basepath} = '/'. join '/', @basepath;
  $self->{version} = pop @pathcomp;
  $self->{pt_terminal} = pop @pathcomp;
  $self->{namespace}  = $pathcomp[0];
  $self->{pt_path_objid} = File::Pairtree::ppath2id(join("/",@pathcomp));
}

sub fileparse {
  my $self = shift;
  my $path = shift;

  # strip trailing / from path
  my ( $pt_objid, $path, $type ) =
  File::Basename::fileparse( $path, qr/\.mets\.xml/, qr/\.zip/ );
  $path =~ s/\/$//;    # remove trailing /

  $self->{pt_objid} = $pt_objid;
  $self->{path} = $path;
  $self->{type} = $type;
}

sub fileinfo {
  my $file = shift;


}

sub zipinfo {
  my $self = shift;

  return $self->{zipinfo} if defined $self->{zipinfo};
  my $zip_file = "$self->{pt_objid}.zip";
  my $zip_path = "$self->{path}/$zip_file";
  if ( -e $zip_path ) {
    $self->{zipinfo} = {
      'zip_file'    => $zip_file,
      'zip_path'    => $zip_path,
      'zip_seconds' => ( stat($zip_path) )[9],
      'zip_size'    => -s $zip_path,
      'zip_date'    => strftime( "%Y-%m-%d %H:%M:%S", localtime( ( stat($zip_path) )[9] ) )
    };
    return $self->{zipinfo};
  }
}

sub metsinfo {
  my $self = shift;

  return $self->{metsinfo} if defined $self->{metsinfo};
  my $mets_file = "$self->{pt_objid}.mets.xml";
  my $mets_path = "$self->{path}/$mets_file";
  if ( -e $mets_path ) {
    $self->{metsinfo} = {
      'mets_file'    => $mets_file,
      'mets_path'    => $mets_path,
      'mets_seconds' => ( stat($mets_path) )[9],
      'mets_size'     => -s $mets_path,
      'mets_date'     => strftime( "%Y-%m-%d %H:%M:%S", localtime( ( stat($mets_path) )[9] ) )
    };
    return $self->{metsinfo};
  }
}

1;

