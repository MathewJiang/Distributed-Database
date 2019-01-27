#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;

my $read_ratio = 0.8;
my @tests;
while(<STDIN>) {
	if($_ =~ /cache_ratio: (.*) Strategy (.*) total: (.*) # of reads: (.*) # of writes: (.*) cache_size: (.*) ratio: (.*) time taken (.*)/) {
		my %test_entry;
		$test_entry{"cache ratio"} = $1;
		$test_entry{"strategy"} = $2;
		$test_entry{"total op"} = $3;
        $test_entry{"read ratio"} = $7;
		$test_entry{"time"} = $8;
		push @tests, \%test_entry;
	}
}
{
my @None;
foreach my $entry (@tests) {
	if($entry->{'cache ratio'} == 0.1 and $entry->{'read ratio'} == $read_ratio) {
		if($entry->{strategy} eq 'None') {
			push @None, $entry;
		}
	}
}
	print "total op, None\n";
	my $i = 0;
	while($i < scalar @None) {
		print "$None[$i]->{'total op'},$None[$i]->{'time'}\n";	
		$i++;
	}
}
my @ratio = (0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0);
foreach my $cache_ratio (@ratio){
	my @FIFO;
	my @LRU;
	my @LFU;

foreach my $entry (@tests) {
	if($entry->{'cache ratio'} == $cache_ratio and $entry->{'read ratio'} == $read_ratio) {
		if($entry->{strategy} eq 'LRU') {
			push @LRU, $entry;
		}
		if($entry->{strategy} eq 'FIFO') {
			push @FIFO, $entry;
		}
		if($entry->{strategy} eq 'LFU') {
			push @LFU, $entry;
		}
	}
}
my $fifo_size = scalar @FIFO;
my $LRU_size = scalar @LRU;
my $LFU_size = scalar @LFU;

#print "$fifo_size $LRU_size $LFU_size\n";
if($fifo_size == $LRU_size and $fifo_size == $LFU_size) {
	my $i = 0;
	print "fix cache ratio to $cache_ratio and read ratio to $read_ratio\n";
	print "Total OP,FIFO,LRU,LFU\n";
	while($i < $fifo_size) {
		print "$FIFO[$i]->{'total op'},$FIFO[$i]->{'time'},$LRU[$i]->{'time'},$LFU[$i]->{'time'}\n";	
		$i++;
	}

}
}

#print Dumper(\@tests);
