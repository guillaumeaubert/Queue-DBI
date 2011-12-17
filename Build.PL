use 5.006;
use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
	module_name         => 'Queue::DBI',
	license             => 'perl',
	dist_author         => q{Guillaume Aubert <aubertg@cpan.org>},
	dist_version_from   => 'lib/Queue/DBI.pm',
	build_requires      =>
	{
		'Test::More'   => 0,
		'Data::Dumper' => 0,
		'Carp'         => 0,
		'Storable'     => 0,
		'MIME::Base64' => 0,
		'DBD::SQLite'  => 0,
	},
	requires            =>
	{
		'perl' => 5.006,
	},
	add_to_cleanup      =>
	[
		'Queue-DBI-*',
	],
	create_makefile_pl  => 'traditional',
);

$builder->create_build_script();
