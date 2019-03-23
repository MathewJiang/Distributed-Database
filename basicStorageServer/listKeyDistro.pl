#!/usr/bin/perl

use strict;
use warnings;

my @servers = `readlink -f ./kvdb/*`;

my $sum = 0;
foreach my $server(@servers) {
	chomp $server;
	
	if($server =~ /.*\/kvdb\/(.*)/) {
		my $key_count = `ls $server | wc -l`;
		print "$1: $key_count";
		chomp $key_count;
		if($server !~ /.*replica.*/) {
			$sum += $key_count;
		}
	}
}

print "total key count: $sum\n";
