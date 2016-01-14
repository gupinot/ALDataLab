#!/usr/bin/perl

use Data::Dumper;

# BT conventions for ports 
%zoneMap = ("1" => "front", "2" => "back");
%envMap = (0 => "ECC", 1 => "UM", 2 => "ECF", 4 => "ECM");
%protocolMap = ("00" => "HTTP", "01" => "HTTPS");

$,="\t";
$host=shift;
$DATADIR=glob("~/content");
$remotecmd="";
$ttyremotecmd="";
if (defined($host))
{
  $remotecmd="ssh $host ";
  $ttyremotecmd="echo '' | ssh -tt $host ";
}
else
{
  $host="localhost";
}

my %equip;
$equip{type}="server";
$equip{interfaces}={};
$equip{routes}={};
$equip{vhosts}=[];

print "$remotecmd hostname \n"; 
open(HOST,"$remotecmd hostname |") || die ("Failed to exec $remotecmd hostname");
chomp($hostname = <HOST>);
close(HOST);
$equip{name}=$hostname;

@hostips=();
print "$remotecmd /sbin/ifconfig -a \n"; 
open(NETIF,"$remotecmd /sbin/ifconfig -a |") || die ("Failed to exec $remotecmd /sbin/ifconfig -a");
chomp(@iflines = <NETIF>);
close(NETIF);
for my $l (@iflines)
{
  if ($l =~ m/^(eth[0-9.:]+)\s/) { $ifname=$1; $addr = undef; $mask = undef; }
  if ($l =~ m/inet addr:([0-9.]+).*Mask:([0-9.]+)/) { $addr = $1; $mask=$2; }
  if ($l =~ m/^\s*$/ && $addr && $ifname)
  {
    $equip{interfaces}->{$ifname}={addr=>$addr,mask=>$mask};
    $ifname = undef;
    push @hostips, $addr;
  }
}
@iflines = undef;
print "$remotecmd netstat -rn \n"; 
open(NETSTAT,"$remotecmd netstat -rn |") || die ("Failed to exec $remotecmd netstat -rn");
chomp(@netlines = <NETSTAT>);
close(NETSTAT);
for my $l (@netlines)
{
  if ($l =~ m/([0-9.]+)\s+([0-9.]+)\s+([0-9.]+).*(eth[0-9.:]+)$/)
  {
    push(@{$equip{routes}->{$4}},{net=>$1,mask=>$3,gw=>$2}) if ($1 eq "0.0.0.0"  or $2 ne "0.0.0.0");
  }
}
@netlines = undef;

print "$remotecmd ps -ef | egrep '(httpd|apache2)' | grep -v grep | sed -e 's/.*-f //' | sort -u \n"; 
open(HTTPLIST, "$remotecmd ps -ef | egrep '(httpd|apache2)' | grep -v grep | sed -e 's/.*-f //' | sort -u |") || die ("Failed to exec $remotecmd ps -ef | grep httpd_ | grep -v grep | sed -e 's/.*-f //' | sort -u |");
chomp(@serverconfs = <HTTPLIST>);
close(HTTPLIST);

for my $conf (@serverconfs)
{
print "$remotecmd /usr/sbin/apachectl -S -f $conf  \n"; 
open(VHOSTS, "$remotecmd /usr/sbin/apachectl -S -f $conf |") || die ("Failed to exec $remotecmd /usr/sbin/apachectl -S -f $conf");
chomp(@v = <VHOSTS>);
push @vhosts, @v;
close(VHOSTS);
}

for my $line (@vhosts)
{
  next if $line =~ m/^Virtual/;
  if ($line =~ m/^((\S+):([0-9]+))\s/)
  {
     # New VirtualHost IP/port section
     $addr = $2;
     $port = $3;
  }
  elsif ($line =~ m/^\s+port ([0-9]+)\s+namevhost (\S+)\s/)
  {  
     push @{$equip{vhosts}},{addr=>$addr,port=>$port,vhost=>$2} if $addr and $port;
  }
  elsif ($line =~ m/^Syntax/)
  {
    $addr = undef;
    $port = undef;
  }
}
@serverconf = undef;
@vhosts = undef;

print "$ttyremotecmd sudo -S /usr/sbin/lsof -i -nP | awk '{print \$1,\$7,\$8,\$9,\$2}' | sort -u  | egrep '(TCP|UDP)' \n"; 
open(LSOF, "$ttyremotecmd sudo -S /usr/sbin/lsof -i -nP| awk '{print \$1,\$7,\$8,\$9,\$2}' | sort -u  | egrep '(TCP|UDP)' |") || die ("Failed to exec $remotecmd lsof -i -nP | awk '{print $1,$8,$9,$10}' | sort -u  | egrep '(TCP|UDP)' |");
chomp(@lsof = <LSOF>);
close(LSOF);

print join("\n",@lsof);
%lsof_ports = ();
%lsof_connects = ();
for my $line (@lsof)
{
  @comp = split / +/, $line;
  $proto = lc($comp[1]);
  $pname = $comp[0];
  $pid = $comp[4];
  ($local,$remote) = split /->/, $comp[2];
  ($local_ip,$local_port) = split /:/, $local;
  ($remote_ip,$remote_port) = split /:/, $remote if $remote;
  $key = $local_port.'_'.$proto;

  if ($line =~ m/LISTEN/)
  {
	$lsof_ports{$key} = {pid => {}, ppid => {}, name => $pname, cmdline => "", ip => {}} if (!defined($lsof_ports{$key}));

        $lsof_ports{$key}->{pid}->{$pid}=1;
 
	if ($local_ip eq "*")
	{
 		for my $i (@hostips)
                {
		   $lsof_ports{$key}->{ip}->{$i} = $i;
                }
	} 
	elsif (!defined($lsof_ports{$key}->{ip}->{$local_ip}))
	{
		$lsof_ports{$key}->{ip}->{$local_ip} = $local_ip;
	}
  }
}

%processes = ();
print "$remotecmd ps -ef \n"; 
open(PS, "$remotecmd ps -ef |") || die ("Failed to exec $remotecmd ps -ef |");
while(<PS>)
{
   chomp;
   m/\S+\s+(\S+)\s+(\S+).*[0-9]+:[0-9]{2}:[0-9]{2} (.*)$/;
   $processes{$1}={ppid=>$2,cmd=>$3};
   
}
close(PS);

for my $k (keys %lsof_ports)
{
   for my $p (keys %{$lsof_ports{$k}->{pid}})
   {
      $ppid = $processes{$p}->{ppid};
      if (defined($lsof_ports{$k}->{pid}->{$ppid}))
      {
		$lsof_ports{$k}->{ppid}->{$p} = $p;
		delete $lsof_ports{$k}->{pid}->{$p};
      }
      else
      {
         $lsof_ports{$k}->{cmdline} = $processes{$p}->{cmd};
      }
   }
}

%processes = undef;

%services = ();
for my $k (keys %lsof_ports)
{
  ($port,$proto) = split (/_/, $k);
  for my $pid  (keys %{$lsof_ports{$k}->{pid}})
  {
     $services{$pid} = {pid => $pid, ppid => {}, port => $port, name => $lsof_ports{$k}->{name}.'-'.$k, cmd => $lsof_ports{$k}->{cmdline}} unless defined $services{$pid};
     if ($port < $services{$pid}->{port} )
     {
          $services{$pid}->{name}=$lsof_ports{$k}->{name}.'-'.$k ;
          $services{$pid}->{port}=$port ;
     }
     for my $ip (keys %{$lsof_ports{$k}->{ip}})
     {
	push @{$services{$pid}->{ports}}, $ip.':'.$k;
     }
     for my $ppid (keys %{$lsof_ports{$k}->{ppid}})
     {
	$services{$pid}->{ppid}->{$ppid}=$ppid;
     }
  }
}

for my $line (@lsof)
{
  @comp = split / +/, $line;
  $proto = lc($comp[1]);
  $pid = $comp[4];
  ($local,$remote) = split /->/, $comp[2];
  ($local_ip,$local_port) = split /:/, $local;
  ($remote_ip,$remote_port) = split /:/, $remote if $remote;
  $key = $local_port.'_'.$proto;
  $pname=$comp[0];
  $local_service = "";
  $remote_service = "";

   if ($remote && ($local_ip eq $remote_ip))
   {
	# local server connection, check if we can guess remote_service
	$rkey = $remote_port.'_'.$proto;
        if (defined($lsof_ports{$rkey}) && (defined($lsof_ports{$rkey}->{ip}->{$remote_ip})))
	{
		$remote_service = $lsof_ports{$rkey}->{name}.'-'.$rkey;
	}
  }

  if (defined($lsof_ports{$key}) && (defined($lsof_ports{$key}->{ip}->{$local_ip})))
   {
	$local_service = $lsof_ports{$key}->{name}.'-'.$key;
   }

  if ($remote)
  {  
	$to = "remote";
	$from = "local";
  
	if ($local_service ne "")
        {
	    $to = "local";
	    $from = "remote";
	}
        
        if ($local_service eq "")
        {
		if (defined($services{$pid}))
		{
			$local_service = $services{$pid}->{name};
			print STDERR "Found $local_service using direct PID lookup $pid\n";
		}
		else
		{
 			# look for service through pid since we couldn't find it using its port
			for my $srv (values %services)
                	{
				if ($srv->{ppid}->{$pid})
				{
					$local_service = $srv->{name};
					print STDERR "Found $local_service using ppid lookup of $pid in service ".$srv->{pid}." \n";
					last;
                       	 	}
			}
		}
	}
	#Build connection info
	$to_port = ${$to."_port"};
	$to_ip = ${$to."_ip"};
	$from_ip = ${$from."_ip"};
	$from_port = ${$from."_port"};
	$to_service = ${$to."_service"};
	$from_service = ${$from."_service"};
        $key="$to_ip-$to_port-$from_ip";
	print STDERR "$pid $pname $from_ip:$from_port($from_service) -> $to_ip:$to_port ($to_service)\n";
	$lsof_connects{$key} = { service => $to_service, fromservice => $from_service, from => $from_ip, to => $to_ip, port => $to_port }; 
  }
}

@lsof = undef;
%lsof_ports = undef;

print "Generating $DATADIR/$host/$hostname.xml...\n";
open(XMLOUT,">$DATADIR/$host/$hostname.xml") || die("$DATADIR/$host/$hostname.xml");
print XMLOUT <<EOF;
<equipment name="$equip{name}" type="$equip{type}">
EOF
for my $ifname (sort keys %{$equip{interfaces}})
{
print XMLOUT <<EOF;
  <interface name="$ifname" ip="$equip{interfaces}->{$ifname}->{addr}" mask="$equip{interfaces}->{$ifname}->{mask}">
EOF
for my $route (@{$equip{routes}->{$ifname}})
{
print XMLOUT <<EOF;
     <route dst="$route->{net}" mask="$route->{mask}" gw="$route->{gw}" />
EOF
}
print XMLOUT <<EOF;
  </interface>
EOF
}

for my $vhost (@{$equip{vhosts}})
{
 my ($zone, $env, $protocol, $version) = decodePort($vhost->{port}); 
print XMLOUT <<EOF;
  <vhost name="$vhost->{vhost}" port="$vhost->{port}" zone="$zone" env="$env" protocol="$protocol" version="$version"/>
EOF
}

print XMLOUT <<EOF;
  <services>
EOF

for my $service (values %services)
{
  $name = $service->{name};
  ($cmd = $service->{cmd}) =~ s/"/\\"/g;
	print XMLOUT <<EOF;
    <service name="$name" cmd="$cmd" >
EOF
   for my $port (@{$service->{ports}}) 
   {
      print XMLOUT <<EOF;
       <port>$port</port>
EOF
   }
print XMLOUT <<EOF;
    </service>
EOF

}

print XMLOUT <<EOF;
  </services>
EOF

print XMLOUT <<EOF;
  <connections>
EOF

for my $connect (values %lsof_connects)
{
	print XMLOUT <<EOF;
     <connection service="$connect->{service}" fromservice="$connect->{fromservice}" from="$connect->{from}" to="$connect->{to}" toport="$connect->{port}" />
EOF
}

print XMLOUT <<EOF;
  </connections>
EOF

print XMLOUT <<EOF;
</equipment>
EOF
close(XMLOUT);

sub decodePort
{
  my $port = shift;
  my $zone = "front";
  my $env = "";
  my $protocol = "HTTP";
  my $version = "n";

  my @digits = split(//,$port);
  $zone = $zoneMap{$digits[0]} if defined($zoneMap{$digits[0]});
  $env = $envMap{$digits[1]} if defined($envMap{$digits[1]});
  $protocol = $protocolMap{$digits[2].$digits[3]} if defined($protocolMap{$digits[2].$digits[3]}); 
  $version = $version."+".$digits[4] if $digits[4] > 0; 

  return ($zone,$env,$protocol,$version);
}
