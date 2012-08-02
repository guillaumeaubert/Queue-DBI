#!perl -T

use strict;
use warnings;

use Test::Exception;
use Test::More tests => 6;

use DBI;
use Queue::DBI;


ok(
	my $dbh = DBI->connect(
		'dbi:SQLite:dbname=t/test_database',
		'',
		'',
		{
			RaiseError => 1,
		}
	),
	'Create connection to a SQLite database.',
);

dies_ok(
	sub
	{
		# Disable printing errors out since we expect the test to fail.
		local $dbh->{'PrintError'} = 0;
		
		$dbh->selectrow_array( q| SELECT * FROM queues | );
	},
	'The queues table does not exist yet.',
);

dies_ok(
	sub
	{
		# Disable printing errors out since we expect the test to fail.
		local $dbh->{'PrintError'} = 0;
		
		$dbh->selectrow_array( q| SELECT * FROM queue_elements | );
	},
	'The queue elements table does not exist yet.',
);

lives_ok(
	sub
	{
		Queue::DBI::create_tables(
			dbh           => $dbh,
			drop_if_exist => 1,
			sqlite        => 1,
		);
	},
	'Create tables.',
);

lives_ok(
	sub
	{
		$dbh->selectrow_array( q| SELECT * FROM queues | );
	},
	'The queues table exists.',
);

lives_ok(
	sub
	{
		$dbh->selectrow_array( q| SELECT * FROM queue_elements | );
	},
	'The queue elements table exists.',
);

