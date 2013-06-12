#!/usr/bin/perl

use strict;
use HTFeed::DBTools qw(get_dbh);
use File::Pairtree qw(s2ppchars id2ppath);
use POSIX qw(strftime);
use File::Basename;

use HTFeed::Volume;
use Digest::MD5;

my $move = shift @ARGV;

sub get_artist {
    my $mets = shift;
    return `grep linkingAgentIdentifierValue '$mets'`;
}

while(my $line = <>) {
    chomp($line);
    my ($barcode) = ($line =~ /(\d{14})$/);
    next unless $barcode;
    my $state = 'UNKNOWN';
    my $pt_objid = s2ppchars($barcode);
    my $bookpath = "/sdr1/obj/mdp/" . id2ppath($barcode) . "/$pt_objid";
    my ($google_state,$error_pct,$conditions,$src_lib) = 
    get_dbh()->selectrow_array("select state,overall_error,conditions,src_lib_bibkey from feed_grin where id = '$barcode' and namespace = 'mdp'");
    my ($queue_status,$lastupdate_days) = get_dbh()->selectrow_array("select status,datediff(CURRENT_TIMESTAMP,update_stamp) from feed_queue where id = '$barcode' and namespace = 'mdp'");
    my ($blacklisted) = get_dbh()->selectrow_array("select count(*) from feed_blacklist where id = '$barcode' and namespace = 'mdp'");
#        my ($error_message) = get_dbh()->selectrow_array("select description from errors e where barcode = '$barcode' order by lastupdate desc limit 1");

    if($blacklisted) {
        $state = 'BLACKLISTED';
    }
    elsif(not defined $queue_status or $queue_status eq 'done' or $queue_status eq 'collated' or $queue_status eq 'rights') {
        if(not defined $google_state) {
            $state = "NOT_IN_GRIN";

        }
        elsif($google_state eq 'CHECKED_IN' or $google_state eq 'NOT_AVAILABLE_FOR_DOWNLOAD') {
            $state = $google_state;
        }
        elsif($error_pct >= 15) {
            $state = 'HIGH_ERROR';
        }
        elsif(defined $conditions and $conditions =~ /31/ and defined $src_lib) {
            $state = 'DUPLICATE';
        }
        else {
            $state = 'UNKNOWN';
        }
    } else {
# In queue..
        if( ($queue_status eq 'available' or $queue_status eq 'in_process') and $lastupdate_days > 7) {
            $state = 'STUCK_IN_PROCESS';
        } elsif($queue_status eq 'punted') {
            my ($msg,$field) = get_dbh()->selectrow_array("select message, field from feed_log where id = '$barcode' and namespace = 'mdp' and level = 'ERROR' order by timestamp desc limit 1");
            $state = "ERROR $msg $field";
        } elsif($lastupdate_days > 2) {
            $state = 'STUCK_IN_QUEUE';
        } else {
            $state = 'IN_QUEUE';
        }
    }

    if(-d $bookpath && -e "$bookpath/$pt_objid.mets.xml") {
        $state = '' if($state eq 'UNKNOWN');
        my $date = (stat("$bookpath/$pt_objid.zip"))[9];
        $date = strftime('%Y-%m-%d',localtime($date));
# try to grep out artist
        my $artist = get_artist("$bookpath/$pt_objid.mets.xml");
        if($artist =~ /Trigonix/im or $artist =~ /Michigan/im or $artist =~ /Zeutschel/ or $artist =~ /MiU/i) {
            $state = "$state DCU\t$date";
        } elsif($artist =~ /Google/i or $artist =~ /CaMv-Goo/i) {
            $state = "$state GOOGLE\t$date";
        } else {
            $state = "$state UNKNOWN_ARTIST\t$date";
        }
        
        my $repo_image_md5_sums;
        my $src_image_md5_sums;
        eval{
            # get repo checksums
            my $volume = HTFeed::Volume->new(packagetype => 'ht', namespace => 'mdp', objid => $barcode);
            my $repo_image_files = $volume->get_file_groups()->{image}->get_filenames();
			my $repo_cheksums = $volume->get_checksums();
            %{$repo_image_md5_sums} = map {$_ => $repo_cheksums->{$_}} @{$repo_image_files};

            # get source checksums
            my $files = get_all_directory_files($line);
            my %src_image_md5_sums = map {$_ => md5sum("$line/$_")} @{$files};
            $src_image_md5_sums = \%src_image_md5_sums
        };
        if($@ or !$repo_image_md5_sums or !$src_image_md5_sums) {
            $state .= "\tMD5_CHECK_ABORTED";
            warn "MD5_CHECK_ABORTED: $@";
        } else {
            # compare chksum hashes
            
            
            if (hash_cmp($repo_image_md5_sums,$src_image_md5_sums)) {
                $state .= "\tMD5_CHECK_OK";
            } else {
                $state .= "\tMD5_CHECK_NOT_OK";
            }
        }
        
    } else {
        $state .= "\tNULL"; #no ingest date
    }

    my @dates = 
    get_dbh()->selectrow_array("select scan_date,process_date,analyze_date,convert_date,dl_date from feed_grin where id = '$barcode' and namespace = 'mdp'");

    if($move and $state =~ /DCU/) {
        my $dirname = dirname($line);
        print "mkdir -p \"$move/$dirname\"\n";
        print "mv $line \"$move/$dirname\"\n";
    } 

    if(!$move) {
        print join("\t",$line,$state,@dates), "\n";
    }

}


# return list of image files in a directory
sub get_all_directory_files {
    my $dir = shift;
    my @directory_files;
    opendir(my $dh,$dir) or die("Can't opendir $dir: $!");

    foreach my $file (readdir $dh) {
        push(@directory_files,$file) 
        if $file =~ /^\d{8}\.(jp2|tif)$/;
    }
    closedir($dh) or croak("Can't closedir $dir: $!");

    @directory_files = sort @directory_files;

    return \@directory_files;
}

sub md5sum {
    my $file = shift;
    my $ctx  = Digest::MD5->new();
    my $fh;
    open( $fh, "<", $file ) or croak("Can't open $file: $!");
    $ctx->addfile($fh);
    close($fh);
    return $ctx->hexdigest();
}

# return true iff hashes are "equal"
# only works on single level hashrefs, with string keys and values
# DESTROYS SECOND ARRAY
sub hash_cmp {
    my $a = shift;
    my $b = shift;
    return unless (ref($a) and ref($b) and ref($a) eq 'HASH' and ref($b) eq 'HASH' and (scalar keys %$a) == (scalar keys %$b));
    foreach (keys %{$a}) {
        return unless ($a->{$_} eq $b->{$_});
    }

    return 1;
}
