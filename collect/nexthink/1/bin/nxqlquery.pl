#!/usr/bin/perl

use strict;
use warnings;
use WWW::Mechanize;
use URI::Escape;

my $format=$ARGV[3];
my $filename=$ARGV[2];
my $NxqlQuery=$ARGV[1];
my $ServerName=$ARGV[0];

my $EncodedNxqlQuery = uri_escape($NxqlQuery);

my $Url = "https://$ServerName:1671/2/query?query=$EncodedNxqlQuery&format=$format";

my $agent = WWW::Mechanize->new(ssl_opts => { verify_hostname => 0});

$agent->agent('Mozilla/5.0');
$agent->timeout(30000);
$agent->max_size(30000000000);
#$agent->protocols_allowed( [ 'http','https'] );
$agent->credentials('admin', 'admin');

my $response = $agent->get( $Url, ':content_file' => $filename );

if ($response->is_success) {

    #$response->save_content( $filename, binmode => ':raw');
    print "$filename created. \n";
}
else {
    die $response->status_line;
}

