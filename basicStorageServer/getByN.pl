#!/usr/bin/perl

use strict;
use warnings;
use threads;


sub work {
	print "working\n";
	my @time = `time java -jar m2-client.jar get.txt > /dev/null`;
	print "work finished in @time\n";
	return
}


my $i = 0;
my @threads;

print "Running $ARGV[0] clients\n";
while($i < $ARGV[0]) {
	my $thread = threads->create(\&work);
	push @threads, $thread;
	$i ++;
}

foreach my $thread(@threads) {
	$thread->join();
}


