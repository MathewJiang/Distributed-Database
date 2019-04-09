#!/usr/bin/perl

use strict;
use warnings;
use threads;


sub work {
	print "working\n";
	my $id = shift @_;
	my $file = `shuf get.txt > get_cmd_$id`;
	my @time = `time java -jar m2-client.jar get_cmd_$id > /dev/null`;
	print "work finished in @time\n";
	return
}


my $i = 0;
my @threads;

print "Running $ARGV[0] clients\n";
while($i < $ARGV[0]) {
	my $thread = threads->create(\&work, $i);
	push @threads, $thread;
	$i ++;
}

foreach my $thread(@threads) {
	$thread->join();
}


