#!/usr/bin/perl -w

##################################################################################################
#####WRITTEN BY ERIC LYONS 3/2018 for CASA, UNIVERSITY OF MASSACHUSETTS##########################
##################################################################################################
#  TESTED FUNCTIONALITY:         
#  Monitors nowcast directory
#  pqinserts nowcast files
#  plots nowcast files
#  pqinserts nowcast images
# 
#  #                                                                                                  #
##################################################################################################

use POSIX qw(setsid);
use File::Copy;
use File::Monitor;
use threads;
use threads::shared;

our $input_data_dir;
our $workflow_dir = $ENV{'NOWCAST_WORKFLOW_DIR'};

&command_line_parse;

&daemonize;

my $file_mon = new threads \&file_monitor;

sleep 900000000;

sub file_monitor {
    
    my $dir_monitor = File::Monitor->new();
        
    $dir_monitor->watch( {
	name        => "$input_data_dir",
	recurse     => 1,
        callback    => \&new_files,
    } );
    
    $dir_monitor->scan;
    
    for ($i=0; $i < 9000000000; $i++) {
	my @changes = $dir_monitor->scan;   
	sleep 10;
    }
    
    sub new_files 
    {
	my ($name, $event, $change) = @_;
	my @tmp = ();
	
	@new_netcdf_files = $change->files_created;
	my @dels = $change->files_deleted;
	print "Added: ".join("\nAdded: ", @new_netcdf_files)."\n" if @new_netcdf_files;
	foreach $file (@new_netcdf_files) {
	    sleep 1;
	    my $pathstr;
            my $filename;
            ($pathstr, $filename) = $file =~ m|^(.*[/\\])([^/\\]+?)$|;
	    my $filetype = "MERGE_DARTS";
	    if (index($filename, $filetype) != -1) {
		my $cploc = $workflow_dir . "/input/" . $filename;
		copy($file, $cploc);
		&trigger_pegasus($filename);
		unlink $file;
	    }
	}
    }
}

sub daemonize {
    chdir '/'                 or die "Can't chdir to /: $!";
    open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
    open STDOUT, '>>/dev/null' or die "Can't write to /dev/null: $!";
    open STDERR, '>>/dev/null' or die "Can't write to /dev/null: $!";
    defined(my $pid = fork)   or die "Can't fork: $!";
    exit if $pid;
    setsid                    or die "Can't start a new session: $!";
    umask 0;
}

sub command_line_parse {
    if ($#ARGV < 0) { 
	print "Usage:  dir_mon.pl netcdf_dir\n";
   	exit; 
    }
    $input_data_dir = $ARGV[0];
    
    my @rdd = split(/ /, $input_data_dir);
    foreach $w (@rdd) {
	print "Will recursively monitor $w for incoming netcdf files\n";
    }
}

sub trigger_pegasus {
    my @ifiles = @_;
    my $ifile = $ifiles[0];
    my $daxcall = $workflow_dir . "/run_wf.sh " . $ifile;
    #print($daxcall);
    system($daxcall);
}
