#!perl -T

use Test::More tests => 1;

BEGIN
{
	use_ok( 'Queue::DBI::Admin' );
}

diag( "Testing Queue::DBI::Admin $Queue::DBI::Admin::VERSION, Perl $], $^X" );
