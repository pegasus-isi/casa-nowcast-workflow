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

##Parse Command Line
&command_line_parse;

&daemonize;

##Realtime Mode -- Gets MCC stream
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
	sleep 3;
    }
    
    sub new_files 
    {
	my ($name, $event, $change) = @_;
	#my %minhash;
	#$minhash{"0min"} = 1;
	#$minhash{"1min"} = 1;
	#$minhash{"5min"} = 1;
	#$minhash{"10min"} = 1;
	#$minhash{"15min"} = 1;

	@new_netcdf_files = $change->files_created;
	my @dels = $change->files_deleted;
	print "Added: ".join("\nAdded: ", @new_netcdf_files)."\n" if @new_netcdf_files;
	foreach $file (@new_netcdf_files) {
	    my $pathstr;
            my $filename;
            ($pathstr, $filename) = $file =~ m|^(.*[/\\])([^/\\]+?)$|;
            my $suffix = substr($filename, -3, 3);
	    
	    if ($suffix eq ".nc") {
		unlink $file;
	    }
	    elsif ($suffix eq "png") {
		my @split_arr = split /_/, $filename;
		my $minstr = substr($split_arr[1], 0, -3);
		if ($minstr < 10) { 
		    $minstr = "00" . $minstr; 
		}
	        elsif ($minstr < 100) { 
		    $minstr = "0" . $minstr; 
		}
		
		my $hmsstr = substr($filename, -10, 6);
		my $ymdstr = substr($filename, -19, 8);
		my $pngpqins = "pqinsert -f EXP -p nowcast_" . $ymdstr . "-" . $hmsstr . "-" . $minstr . ".png " . $file;
	        #print $pngpqins;
		system($pngpqins);
		#sleep 1;
		unlink $file;
	    }
	    elsif ($suffix eq "son") {
		
		my @split_arr = split /_/, $filename;
		my $minstr = $split_arr[3];
		if ($minstr < 10) {
		    $minstr = "00" . $minstr;
		}
		elsif ($minstr < 100) {
		    $minstr = "0" . $minstr;
		}
		my @split_arr_2 = split /-/, $split_arr[4];
		my $ymdstr = $split_arr_2[0];
		my $hmsstr = substr($split_arr_2[1], 0, 6);
		#print "ymdstr: " . $ymdstr . " hmsstr: " . $hmsstr . " minstr: " . $minstr . "\n";
		my $jsonpqins = "pqinsert -f EXP -p nowcast_" . $ymdstr . "-" . $hmsstr . "-" . $minstr . ".geojson "  . $file;
		#print $jsonpqins;
		system($jsonpqins);
		#sleep 1;
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
