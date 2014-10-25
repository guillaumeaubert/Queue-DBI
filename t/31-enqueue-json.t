#!perl -T

use strict;
use warnings;

use Test::Exception;
use Test::FailWarnings;
use Test::More;

use lib 't/';
use LocalTest;

use Queue::DBI;


# Only run this test if a JSON module is available.
eval "use JSON::MaybeXS";
plan( skip_all => "JSON::MaybeXS is not installed." )
	if $@;

plan( tests => 12 );

my $dbh = LocalTest::ok_database_handle();
my $json = JSON::MaybeXS->new();

# Instantiate the queue object.
my $queue;
lives_ok(
	sub
	{
		$queue = Queue::DBI->new(
			'queue_name'        => 'test1',
			'database_handle'   => $dbh,
			'cleanup_timeout'   => 3600,
			'verbose'           => 0,
			'serializer_freeze' => sub { $json->encode($_[0]) },
			'serializer_thaw'   => sub { $json->decode($_[0]) },
		);
	},
	'Instantiate a new Queue::DBI object.',
);

# Test data.
ok(
	defined(
		my $data =
		{
			block => 49494494,
		}
	),
	'Define test data.',
);

# Test freezing/unfreezing.
my $frozen_data;
lives_ok(
	sub
	{
		$frozen_data = $queue->freeze( $data );
	},
	'Freeze the data.',
);
like(
	$frozen_data,
	qr/^\{\W*block\W*:\W*49494494\W*\}/,
	'The frozen data looks like a JSON string.',
);
my $thawed_data;
lives_ok(
	sub
	{
		$thawed_data = $queue->thaw( $frozen_data ),
	},
	'Thaw the frozen data.',
);
is_deeply(
	$thawed_data,
	$data,
	'The thawed data matches the original data.',
);

# Insert data.
lives_ok(
	sub
	{
		$queue->enqueue( $data );
	},
	'Queue data.',
);

# Retrieve data.
my $queue_element;
lives_ok(
	sub
	{
		$queue_element = $queue->next();
	},
	'Call to retrieve the next item in the queue.',
);
isa_ok(
	$queue_element,
	'Queue::DBI::Element',
	'Object returned by next()',
);

# Lock.
lives_ok(
	sub
	{
		$queue_element->lock()
		||
		die 'Cannot lock element';
	},
	'Lock element.',
);

# Remove.
lives_ok(
	sub
	{
		$queue_element->success()
		||
		die 'Cannot mark as successfully processed';
	},
	'Mark as successfully processed.',
);
