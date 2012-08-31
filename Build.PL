use 5.006;
use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
	module_name         => 'Queue::DBI',
	license             => 'artistic',
	dist_author         => q{Guillaume Aubert <aubertg@cpan.org>},
	dist_version_from   => 'lib/Queue/DBI.pm',
	build_requires      =>
	{
		'DBD::SQLite'          => 0,
		'DBI'                  => 0,
		'Test::More'           => 0.94,
		'Test::Exception'      => 0,
	},
	requires            =>
	{
		'perl'                 => 5.006,
		'Data::Dumper'         => 0,
		'Carp'                 => 0,
		'Storable'             => 0,
		'MIME::Base64'         => 0,
		'Data::Validate::Type' => 0,
		'Try::Tiny'            => 0,
	},
	add_to_cleanup      =>
	[
		'Queue-DBI-*',
	],
	create_makefile_pl  => 'traditional',
	configure_requires  =>
	{
		'Module::Build'        => 0,
	},
	meta_merge          =>
	{
		resources              =>
		{
			repository  => 'https://sourceforge.net/p/queuedbiperl/code',
			homepage    => 'https://metacpan.org/release/Queue-DBI',
			bugtracker  => 'http://rt.cpan.org/NoAuth/ReportBug.html?Queue=queue-dbi',
		},
	},
);

$builder->create_build_script();
