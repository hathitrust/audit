package DataDenPath;

use File::Basename;

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

  $self->{basepath} = shift @pathcomp;
  $self->{version} = pop @pathcomp;
  $self->{pt_terminal} = pop @pathcomp;
  $self->{namespace}  = $pathcomp[1];
  $self->{pt_path_objid} = ppath2id(join("/",@pathcomp));
}

sub fileparse {
  my $self = shift;
  my $path = shift;

  # strip trailing / from path
  my ( $pt_objid, $path, $type ) =
  fileparse( $line, qr/\.mets\.xml/, qr/\.zip/ );
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
  
  #get last modified date
  my $zipfile = "$self->{path}/$self->{pt_objid}.zip";
  my $zip_seconds;
  my $zipdate;
  my $zipsize;

  if ( -e $zipfile ) {
    $zip_seconds = ( stat($zipfile) )[9];
    $zipsize = -s $zipfile;
    $zipdate = strftime( "%Y-%m-%d %H:%M:%S", localtime($zip_seconds) );
  }
}

sub metsinfo {
  my $self = shift;

  my $metsfile = "$self->{path}/$self->{pt_objid}.mets.xml";

  my $mets_seconds;
  my $metsdate;
  my $metssize;

  if ( -e $metsfile ) {
    $mets_seconds = ( stat($metsfile) )[9];
    $metssize     = -s $metsfile;
    $metsdate     = strftime( "%Y-%m-%d %H:%M:%S",
      localtime( ( stat($metsfile) )[9] ) );
  }
}
