#!/usr/bin/perl

use strict;
use warnings;

my @servers = `readlink -f ./kvdb/*`;

foreach my $server(@servers) {
	chomp $server;
	if($server =~ /.*\/kvdb\/(.*)/) {
		my $key_count = `ls $server | wc -l`;
		print "$1: $key_count";
	}
}
