#!perl -T

use Test::More tests => 1;

BEGIN
{
	use_ok( 'Queue::DBI::Element' );
}

diag( "Testing Queue::DBI::Element $Queue::DBI::Element::VERSION, Perl $], $^X" );
