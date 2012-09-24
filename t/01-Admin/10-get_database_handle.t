#!perl -T

use strict;
use warnings;

use Test::Exception;
use Test::More tests => 5;

use DBI;
use Queue::DBI::Admin;


ok(
	my $dbh = DBI->connect(
		'dbi:SQLite:dbname=t/01-Admin/test_database',
		'',
		'',
		{
			RaiseError => 1,
		}
	),
	'Create connection to a SQLite database.',
);

can_ok(
	'Queue::DBI::Admin',
	'get_database_handle',
);

my $queue_admin;
lives_ok(
	sub
	{
		$queue_admin = Queue::DBI::Admin->new(
			'database_handle' => $dbh,
		);
	},
	'Instantiate a new Queue::DBI::Admin object.',
);

ok(
	defined(
		my $database_handle = $queue_admin->get_database_handle()
	),
	'Retrieve the value returned by get_database_handle().',
);

isa_ok(
	$database_handle,
	'DBI::db',
	'The return value',
);
