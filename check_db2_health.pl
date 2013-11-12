#! /usr/bin/perl -w

my %ERRORS=( OK => 0, WARNING => 1, CRITICAL => 2, UNKNOWN => 3 );
my %ERRORCODES=( 0 => 'OK', 1 => 'WARNING', 2 => 'CRITICAL', 3 => 'UNKNOWN' );
package DBD::DB2::Server::Instance;

use strict;

our @ISA = qw(DBD::DB2::Server);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    databases => [],
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::sga/) {
    $self->{sga} = DBD::DB2::Server::Instance::SGA->new(%params);
  } elsif (($params{mode} =~ /server::instance::database/) ||
      ($params{mode} =~ /server::instance::listdatabases/)) {
    DBD::DB2::Server::Instance::Database::init_databases(%params);
    if (my @databases =
        DBD::DB2::Server::Instance::Database::return_databases()) {
      $self->{databases} = \@databases;
    } else {
      $self->add_nagios_critical("unable to aquire database info");
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::listdatabases/) {
      foreach (sort { $a->{name} cmp $b->{name}; }  @{$self->{databases}}) {
        printf "%s\n", $_->{name};
      }
      $self->add_nagios_ok("have fun");
    } elsif ($params{mode} =~ /server::instance::database/) {
      foreach (@{$self->{databases}}) {
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    }
  }
}



package DBD::DB2::Server::Instance::Database;

use strict;

our @ISA = qw(DBD::DB2::Server::Instance);

{
  my @databases = ();
  my $initerrors = undef;

  sub add_database {
    push(@databases, shift);
  }

  sub return_databases {
    return reverse
        sort { $a->{name} cmp $b->{name} } @databases;
  }

  sub init_databases {
    my %params = @_;
    my $num_databases = 0;
    if ($params{mode} =~ /server::instance::listdatabases/) {
      my %thisparams = %params;
      $thisparams{name} = $params{database};
      my $database = DBD::DB2::Server::Instance::Database->new(
          %thisparams);
      add_database($database);
      $num_databases++;
    } elsif (($params{mode} =~ /server::instance::database::listtablespace/) ||
        ($params{mode} =~ /server::instance::database::tablespace/) ||
        ($params{mode} =~ /server::instance::database::bufferpool/)) {
      my %thisparams = %params;
      $thisparams{name} = $params{database};
      my $database = DBD::DB2::Server::Instance::Database->new(
          %thisparams);
      add_database($database);
      $num_databases++;
    } elsif ($params{mode} =~ /server::instance::database::lock/) {
      my %thisparams = %params;
      $thisparams{name} = $params{database};
      my $database = DBD::DB2::Server::Instance::Database->new(
          %thisparams);
      add_database($database);
      $num_databases++;
    } elsif (($params{mode} =~ /server::instance::database::usage/)) {
      my @databaseresult = $params{handle}->fetchrow_array(q{
        CALL GET_DBSIZE_INFO(?, ?, ?, 0)
      });
      my ($snapshot_timestamp, $db_size, $db_capacity)  = 
          $params{handle}->fetchrow_array(q{
              SELECT * FROM SYSTOOLS.STMG_DBSIZE_INFO
          });
      if ($snapshot_timestamp) {
        my %thisparams = %params;
        $thisparams{name} = $params{database};
        $thisparams{snapshot_timestamp} = $snapshot_timestamp;
        $thisparams{db_size} = $db_size;
        $thisparams{db_capacity} = $db_capacity;
        my $database = DBD::DB2::Server::Instance::Database->new(
            %thisparams);
        add_database($database);
        $num_databases++;
      } else {
        $initerrors = 1;
        return undef;
      }
    } else {
      my %thisparams = %params;
      $thisparams{name} = $params{database};
      my $database = DBD::DB2::Server::Instance::Database->new(
          %thisparams);
      add_database($database);
      $num_databases++;
    }
  }

}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    name => $params{name},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    tablespaces => [],
    bufferpools => [],
    partitions => [],
    locks => [],
    snapshot_timestamp => $params{snapshot_timestamp},
    db_size => $params{db_size},
    db_capacity => $params{db_capacity},
    log_utilization => undef,
    srp => undef,
    awp => undef,
    rows_read => undef,
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  $self->set_local_db_thresholds(%params);
  if (($params{mode} =~ /server::instance::database::listtablespaces/) ||
      ($params{mode} =~ /server::instance::database::tablespace/)) {
    DBD::DB2::Server::Instance::Database::Tablespace::init_tablespaces(%params);
    if (my @tablespaces = 
        DBD::DB2::Server::Instance::Database::Tablespace::return_tablespaces()) {
      $self->{tablespaces} = \@tablespaces;
    } else {
      $self->add_nagios_critical("unable to aquire tablespace info");
    }
  } elsif (($params{mode} =~ /server::instance::database::listbufferpools/) ||
      ($params{mode} =~ /server::instance::database::bufferpool/)) {
    DBD::DB2::Server::Instance::Database::Bufferpool::init_bufferpools(%params);
    if (my @bufferpools = 
        DBD::DB2::Server::Instance::Database::Bufferpool::return_bufferpools()) {
      $self->{bufferpools} = \@bufferpools;
    } else {
      $self->add_nagios_critical("unable to aquire bufferpool info");
    }
  } elsif ($params{mode} =~ /server::instance::database::lock/) {
    DBD::DB2::Server::Instance::Database::Lock::init_locks(%params);
    if (my @locks = 
        DBD::DB2::Server::Instance::Database::Lock::return_locks()) {
      $self->{locks} = \@locks;
    } else {
      $self->add_nagios_critical("unable to aquire lock info");
    }
  } elsif ($params{mode} =~ /server::instance::database::usage/) {
    # http://publib.boulder.ibm.com/infocenter/db2luw/v9/topic/com.ibm.db2.udb.admin.doc/doc/r0011863.htm
    $self->{db_usage} = ($self->{db_size} * 100 / $self->{db_capacity});
  } elsif ($params{mode} =~ /server::instance::database::logutilization/) {
    DBD::DB2::Server::Instance::Database::Partition::init_partitions(%params);
    if (my @partitions = 
        DBD::DB2::Server::Instance::Database::Partition::return_partitions()) {
      $self->{partitions} = \@partitions;
    } else {
      $self->add_nagios_critical("unable to aquire partitions info");
    }
  } elsif ($params{mode} =~ /server::instance::database::srp/) {
    # http://www.dbisoftware.com/blog/db2_performance.php?id=96
    $self->{srp} = $params{handle}->fetchrow_array(q{
        SELECT 
          100 - (((pool_async_data_reads + pool_async_index_reads) * 100 ) /
          (pool_data_p_reads + pool_index_p_reads + 1)) 
        AS
          srp 
        FROM
          sysibmadm.snapdb 
        WHERE 
          db_name = ?
    }, $self->{name});
    if (! defined $self->{srp}) {
      $self->add_nagios_critical("unable to aquire srp info");
    }
  } elsif ($params{mode} =~ /server::instance::database::awp/) {
    # http://www.dbisoftware.com/blog/db2_performance.php?id=117
    $self->{awp} = $params{handle}->fetchrow_array(q{
        SELECT 
          (((pool_async_data_writes + pool_async_index_writes) * 100 ) /
          (pool_data_writes + pool_index_writes + 1)) 
        AS
          awp 
        FROM
          sysibmadm.snapdb 
        WHERE 
          db_name = ?
    }, $self->{name});
    if (! defined $self->{awp}) {
      $self->add_nagios_critical("unable to aquire awp info");
    }
  } elsif ($params{mode} =~ /server::instance::database::indexusage/) {
    ($self->{rows_read}, $self->{rows_selected}) = $params{handle}->fetchrow_array(q{
        SELECT
            rows_read, (rows_selected + rows_inserted + rows_updated + rows_deleted)
        FROM
            sysibmadm.snapdb
    });
    if (! defined $self->{rows_read}) {
      $self->add_nagios_critical("unable to aquire rows info");
    } else {
      $self->{index_usage} = $self->{rows_read} ? 
          ($self->{rows_selected} / $self->{rows_read} * 100) : 100;
    }
  } elsif ($params{mode} =~ /server::instance::database::connectedusers/) {
    $self->{connected_users} = $self->{handle}->fetchrow_array(q{
        #SELECT COUNT(*) FROM sysibmadm.applications WHERE appl_status = 'CONNECTED'
        # there are a lot more stati than "connected". Applications can
        # be connected although being in another state.
        SELECT COUNT(*) FROM sysibmadm.applications
    });
  } elsif ($params{mode} =~ /server::instance::database::lastbackup/) {
    my $sql = undef;
    if ($self->version_is_minimum('9.1')) {
      $sql = sprintf "SELECT (DAYS(current timestamp) - DAYS(last_backup)) * 86400 + (MIDNIGHT_SECONDS(current timestamp) - MIDNIGHT_SECONDS(last_backup)) FROM sysibm.sysdummy1, TABLE(snap_get_db_v91('%s', -2))", $self->{name};
    } else {
      $sql = sprintf "SELECT last_backup FROM table(snap_get_db('%s', -2))",
          $self->{name};
    }
    $self->{last_backup} = $self->{handle}->fetchrow_array($sql);
    $self->{last_backup} = $self->{last_backup} ? $self->{last_backup} : 0;
    # time is measured in days
    $self->{last_backup} = $self->{last_backup} / 86400;
  } elsif ($params{mode} =~ /server::instance::database::staletablerunstats/) {
    @{$self->{stale_tables}} = $self->{handle}->fetchall_array(q{
      SELECT LOWER(TRIM(tabschema)||'.'||TRIM(tabname)), 
      (DAYS(current timestamp) - DAYS(COALESCE(stats_time, '1970-01-01-00.00.00'))) * 86400 + (MIDNIGHT_SECONDS(current timestamp) - MIDNIGHT_SECONDS(COALESCE(stats_time, '1970-01-01-00.00.00'))) FROM syscat.tables
    });
    if ($params{selectname} && $params{regexp}) {
      @{$self->{stale_tables}} = grep { $_->[0] =~ $params{selectname} }
          @{$self->{stale_tables}};
    } elsif ($params{selectname}) {
      @{$self->{stale_tables}} = grep { $_->[0] eq $params{selectname} }
          @{$self->{stale_tables}};
    }
    # time is measured in days
    @{$self->{stale_tables}} = map { $_->[1] = $_->[1] / 86400; $_; } @{$self->{stale_tables}};
  } elsif ($params{mode} =~ /server::instance::database::sortoverflows/) {
    my $sql = undef;
    if ($self->version_is_minimum('9.1')) {
      $sql = sprintf "SELECT sort_overflows FROM TABLE(snap_get_db_v91('%s', -2))",
          $self->{name};
    } else {
      $sql = sprintf "SELECT sort_overflows FROM TABLE(snap_get_db('%s', -2))",
          $self->{name};
    }
    $self->{sort_overflows} = $self->{handle}->fetchrow_array($sql);
    $self->valdiff(\%params, qw(sort_overflows));
    $self->{sort_overflows_per_sec} = $self->{delta_sort_overflows} / $self->{delta_timestamp};
  } elsif ($params{mode} =~ /server::instance::database::sortoverflowpercentage/) {
    my $sql = undef;
    if ($self->version_is_minimum('9.1')) {
      $sql = sprintf "SELECT sort_overflows, total_sorts FROM TABLE(snap_get_db_v91('%s', -2))",
          $self->{name};
    } else {
      $sql = sprintf "SELECT sort_overflows, total_sorts FROM TABLE(snap_get_db('%s', -2))",
          $self->{name};
    }
    ($self->{sort_overflows}, $self->{total_sorts}) = $self->{handle}->fetchrow_array($sql);
    $self->valdiff(\%params, qw(sort_overflows total_sorts));
    $self->{sort_overflow_percentage} = $self->{delta_total_sorts} == 0 ? 0 :
        $self->{delta_sort_overflows} / $self->{delta_total_sorts};
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::database::listtablespaces/) {
      foreach (sort { $a->{name} cmp $b->{name}; }  @{$self->{tablespaces}}) {
	printf "%s\n", $_->{name};
      }
      $self->add_nagios_ok("have fun");
    } elsif ($params{mode} =~ /server::instance::database::tablespace/) {
      foreach (@{$self->{tablespaces}}) {
        # sind hier noch nach pctused sortiert
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /server::instance::database::listbufferpools/) {
      foreach (sort { $a->{name} cmp $b->{name}; }  @{$self->{bufferpools}}) {
        printf "%s\n", $_->{name};
      }
      $self->add_nagios_ok("have fun");
    } elsif ($params{mode} =~ /server::instance::database::bufferpool/) {
      foreach (@{$self->{bufferpools}}) {
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /server::instance::database::lock/) {
      foreach (@{$self->{locks}}) {
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /server::instance::database::logutilization/) {
      foreach (@{$self->{partitions}}) {
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /server::instance::database::usage/) {
      $self->add_nagios(
          $self->check_thresholds($self->{db_usage}, 80, 90),
          sprintf "database usage is %.2f%%",
              $self->{db_usage});
      $self->add_perfdata(sprintf "\'db_%s_usage\'=%.2f%%;%s;%s",
          lc $self->{name},
          $self->{db_usage},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::instance::database::connectedusers/) {
      $self->add_nagios(
          $self->check_thresholds($self->{connected_users}, 50, 100),
          sprintf "%d connected users",
              $self->{connected_users});
      $self->add_perfdata(sprintf "connected_users=%d;%d;%d",
          $self->{connected_users},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::instance::database::srp/) {
      $self->add_nagios(
          $self->check_thresholds($self->{srp}, '90:', '80:'),       
          sprintf "synchronous read percentage is %.2f%%", $self->{srp});
      $self->add_perfdata(sprintf "srp=%.2f%%;%s;%s",
          $self->{srp},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::instance::database::awp/) {
      $self->add_nagios(
          $self->check_thresholds($self->{awp}, '90:', '80:'),       
          sprintf "asynchronous write percentage is %.2f%%", $self->{awp});
      $self->add_perfdata(sprintf "awp=%.2f%%;%s;%s",
          $self->{awp},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::instance::database::indexusage/) {
      $self->add_nagios(
          $self->check_thresholds($self->{index_usage}, '98:', '90:'),       
          sprintf "index usage is %.2f%%", $self->{index_usage});
      $self->add_perfdata(sprintf "index_usage=%.2f%%;%s;%s",
          $self->{index_usage},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::instance::database::lastbackup/) {
      $self->add_nagios(
          $self->check_thresholds($self->{last_backup}, '1', '2'),
          sprintf "last backup of db %s was %.2f days ago",
              $self->{name}, $self->{last_backup});
      $self->add_perfdata(sprintf "last_backup=%.2f;%s;%s",
          $self->{last_backup},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::instance::database::staletablerunstats/) {
      # we only use warnings here
      $self->check_thresholds(0, 7, 99999);
      @{$self->{stale_tables}} = grep { $_->[1] >= $self->{warningrange} }
          @{$self->{stale_tables}};
      if (@{$self->{stale_tables}}) {
        $self->add_nagios_warning(sprintf '%d tables have outdated statistics', 
            scalar(@{$self->{stale_tables}}));
        foreach (@{$self->{stale_tables}}) {
          $self->add_nagios_warning(sprintf '%s:%.02f', $_->[0], $_->[1]);
        }
      } else {
        $self->add_nagios_ok('table statistics are up to date');
      }
    } elsif ($params{mode} =~ /server::instance::database::sortoverflows/) {
      printf STDERR "%s\n", Data::Dumper::Dumper($self->{sort_overflows_per_sec});
      $self->add_nagios(
          $self->check_thresholds($self->{sort_overflows_per_sec}, 0.01, 0.1),       
          sprintf "%.2f sort overflows per sec", $self->{sort_overflows_per_sec});
      $self->add_perfdata(sprintf "sort_overflows_per_sec=%.2f;%s;%s",
          $self->{sort_overflows_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::instance::database::sortoverflowpercentage/) {
      $self->add_nagios(
          $self->check_thresholds($self->{sort_overflow_percentage}, 5, 10),       
          sprintf "%.2f%% of all sorts used temporary disk space", $self->{sort_overflow_percentage});
      $self->add_perfdata(sprintf "sort_overflow_percentage=%.2f%%;%s;%s",
          $self->{sort_overflow_percentage},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}



package DBD::DB2::Server::Instance::Database::Tablespace;

use strict;

our @ISA = qw(DBD::DB2::Server::Instance::Database);


{
  my @tablespaces = ();
  my $initerrors = undef;

  sub add_tablespace {
    push(@tablespaces, shift);
  }

  sub return_tablespaces {
    return reverse 
        sort { $a->{name} cmp $b->{name} } @tablespaces;
  }
  
  sub init_tablespaces {
    my %params = @_;
    my $num_tablespaces = 0;
    if ($params{mode} =~ /server::instance::database::listtablespaces/) {
      my @tablespaceresult = $params{handle}->fetchall_array(q{
        SELECT tbspace, tbspacetype FROM syscat.tablespaces
      });
      foreach (@tablespaceresult) {
        my ($name, $type) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{type} = $type;
        my $tablespace = DBD::DB2::Server::Instance::Database::Tablespace->new(
            %thisparams);
        add_tablespace($tablespace);
        $num_tablespaces++;
      }
      if (! $num_tablespaces) {
        $initerrors = 1;
        return undef;
      }
    } elsif (($params{mode} =~ /server::instance::database::tablespace::usage/) ||
        ($params{mode} =~ /server::instance::database::tablespace::free/) ||
        ($params{mode} =~ /server::instance::database::tablespace::remainingfreetime/)) {
      # evt snapcontainer statt container_utilization
      my @tablespaceresult = $params{handle}->fetchall_array(q{
        SELECT
            tbsp_name, tbsp_type, tbsp_state, tbsp_usable_size_kb,
            tbsp_total_size_kb, tbsp_used_size_kb, tbsp_free_size_kb
        FROM
            sysibmadm.tbsp_utilization 
        WHERE
            tbsp_type = 'DMS'
        UNION ALL
        SELECT
            tu.tbsp_name, tu.tbsp_type, tu.tbsp_state, tu.tbsp_usable_size_kb,
            tu.tbsp_total_size_kb, tu.tbsp_used_size_kb, 
            (cu.fs_total_size_kb - cu.fs_used_size_kb) AS tbsp_free_size_kb
        FROM
            sysibmadm.tbsp_utilization tu
        INNER JOIN (
            SELECT 
                tbsp_id,
                SUM(fs_total_size_kb) AS fs_total_size_kb,
                SUM(fs_used_size_kb) AS fs_used_size_kb 
           FROM
               sysibmadm.container_utilization
           GROUP BY
               tbsp_id
        ) cu 
        ON
            (tu.tbsp_type = 'SMS' AND tu.tbsp_id = cu.tbsp_id)
      });
      foreach (@tablespaceresult) {
        my ($name, $type, $state, $total_size, $usable_size, $used_size, $free_size) =
            @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{type} = $type;
        $thisparams{state} = lc $state;
        $thisparams{total_size} = $total_size * 1024;
        $thisparams{usable_size} = $usable_size * 1024;
        $thisparams{used_size} = $used_size * 1024;
        $thisparams{free_size} = $free_size * 1024;
        my $tablespace = DBD::DB2::Server::Instance::Database::Tablespace->new(
            %thisparams);
        add_tablespace($tablespace);
        $num_tablespaces++;
      }
      if (! $num_tablespaces) {
        $initerrors = 1;
        return undef;
      }
    } elsif ($params{mode} =~ /server::instance::database::tablespace::datafile/) {
      my %thisparams = %params;
      $thisparams{name} = "dummy_for_datafiles";
      $thisparams{datafiles} = [];
      my $tablespace = DBD::DB2::Server::Instance::Database::Tablespace->new(
          %thisparams);
      add_tablespace($tablespace);
    }
  }
}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    name => $params{name},
    type => ($params{type} =~ /^[dD]/ ? 'dms' : 'sms'),
    state => $params{state},
    total_size => $params{total_size},
    usable_size => $params{usable_size},
    used_size => $params{used_size},
    free_size => $params{free_size},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::database::tablespace::(usage|free)/) {
    if ($self->{type} eq 'sms') {
      # hier ist usable == used
      $self->{usable_size} = $self->{used_size} + $self->{free_size};
    }
    $self->{percent_used} =
        $self->{used_size} * 100 / $self->{usable_size};
    $self->{percent_free} = 100 - $self->{percent_used};
  } elsif ($params{mode} =~ /server::instance::database::tablespace::fragmentation/) {
  } elsif ($params{mode} =~ /server::instance::database::tablespace::datafile/) {
    DBD::DB2::Server::Instance::Database::Tablespace::Datafile::init_datafiles(%params);
    if (my @datafiles =
        DBD::DB2::Server::Instance::Database::Tablespace::Datafile::return_datafiles()) {
      $self->{datafiles} = \@datafiles;
    } else {
      $self->add_nagios_critical("unable to aquire datafile info");
    }
  } elsif ($params{mode} =~ /server::instance::database::tablespace::remainingfreetime/) {
    # load historical data
    # calculate slope, intercept (go back periods * interval)
    # calculate remaining time
    $self->{percent_used} = $self->{bytes_max} == 0 ?
        ($self->{bytes} - $self->{bytes_free}) / $self->{bytes} * 100 :
        ($self->{bytes} - $self->{bytes_free}) / $self->{bytes_max} * 100;
    $self->{usage_history} = $self->load_state( %params ) || [];
    my $now = time;
    if (scalar(@{$self->{usage_history}})) {
      $self->trace(sprintf "loaded %d data sets from     %s - %s", 
          scalar(@{$self->{usage_history}}),
          scalar localtime((@{$self->{usage_history}})[0]->[0]),
          scalar localtime($now));
      # only data sets with valid usage. only newer than 91 days
      $self->{usage_history} = 
          [ grep { defined $_->[1] && ($now - $_->[0]) < 7862400 } @{$self->{usage_history}} ];
      $self->trace(sprintf "trimmed to %d data sets from %s - %s", 
          scalar(@{$self->{usage_history}}),
          scalar localtime((@{$self->{usage_history}})[0]->[0]),
          scalar localtime($now));
    } else {
      $self->trace(sprintf "no historical data found");
    }
    push(@{$self->{usage_history}}, [ time, $self->{percent_used} ]);
    $params{save} = $self->{usage_history};
    $self->save_state(%params);
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::database::tablespace::usage/) {
      $self->add_nagios(
          $self->check_thresholds($self->{percent_used}, "90", "98"),
              sprintf("tbs %s usage is %.2f%%", $self->{name}, $self->{percent_used})
      );
      $self->add_perfdata(sprintf "\'tbs_%s_usage_pct\'=%.2f%%;%d;%d",
          lc $self->{name},
          $self->{percent_used},
          $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(sprintf "\'tbs_%s_usage\'=%dMB;%d;%d;%d;%d",
          lc $self->{name},
          $self->{used_size} / 1048576,
          $self->{warningrange} * $self->{usable_size} / 100 / 1048576,
          $self->{criticalrange} * $self->{usable_size} / 100 / 1048576,
          0, $self->{usable_size} / 1048576);
    } elsif ($params{mode} =~ /server::instance::database::tablespace::free/) {
      # umrechnen der thresholds
      # ()/%
      # MB
      # GB
      # KB
      if (($self->{warningrange} && $self->{warningrange} !~ /^\d+:/) ||
          ($self->{criticalrange} && $self->{criticalrange} !~ /^\d+:/)) {
        $self->add_nagios_unknown("you want an alert if free space is _above_ a threshold????");
        return;
      }
      if (! $params{units}) {
        $params{units} = "%";
      }
      $self->{warning_bytes} = 0;
      $self->{critical_bytes} = 0;
      if ($params{units} eq "%") {
        $self->add_nagios(
            $self->check_thresholds($self->{percent_free}, "5:", "2:"),
                sprintf("tbs %s has %.2f%% free space left", $self->{name}, $self->{percent_free})
        );
        $self->{warningrange} =~ s/://g;
        $self->{criticalrange} =~ s/://g;
        $self->add_perfdata(sprintf "\'tbs_%s_free_pct\'=%.2f%%;%d:;%d:",
            lc $self->{name},
            $self->{percent_free},
            $self->{warningrange}, $self->{criticalrange});
        $self->add_perfdata(sprintf "\'tbs_%s_free\'=%dMB;%.2f:;%.2f:;0;%.2f",
            lc $self->{name},
            $self->{free_size} / 1048576,
            $self->{warningrange} * $self->{usable_size} / 100 / 1048576,
            $self->{criticalrange} * $self->{usable_size} / 100 / 1048576,
            $self->{usable_size} / 1048576);
      } else {
        my $factor = 1024 * 1024; # default MB
        if ($params{units} eq "GB") {
          $factor = 1024 * 1024 * 1024;
        } elsif ($params{units} eq "MB") {
          $factor = 1024 * 1024;
        } elsif ($params{units} eq "KB") {
          $factor = 1024;
        }
        $self->{warningrange} ||= "5:";
        $self->{criticalrange} ||= "2:";
        my $saved_warningrange = $self->{warningrange};
        my $saved_criticalrange = $self->{criticalrange};
        # : entfernen weil gerechnet werden muss
        $self->{warningrange} =~ s/://g;
        $self->{criticalrange} =~ s/://g;
        $self->{warningrange} = $self->{warningrange} ?
            $self->{warningrange} * $factor : 5 * $factor;
        $self->{criticalrange} = $self->{criticalrange} ?
            $self->{criticalrange} * $factor : 2 * $factor;
        $self->{percent_warning} = 100 * $self->{warningrange} / $self->{usable_size};
        $self->{percent_critical} = 100 * $self->{criticalrange} / $self->{usable_size};
        $self->{warningrange} .= ':';
        $self->{criticalrange} .= ':';
        $self->add_nagios(
            $self->check_thresholds($self->{free_size}, "5242880:", "1048576:"),
                sprintf("tbs %s has %.2f%s free space left", $self->{name},
                    $self->{free_size} / $factor, $params{units})
        );
	$self->{warningrange} = $saved_warningrange;
        $self->{criticalrange} = $saved_criticalrange;
        $self->{warningrange} =~ s/://g;
        $self->{criticalrange} =~ s/://g;
        $self->add_perfdata(sprintf "\'tbs_%s_free_pct\'=%.2f%%;%.2f:;%.2f:",
            lc $self->{name},
            $self->{percent_free}, $self->{percent_warning}, 
            $self->{percent_critical});
        $self->add_perfdata(sprintf "\'tbs_%s_free\'=%.2f%s;%.2f:;%.2f:;0;%.2f",
            lc $self->{name},
            $self->{free_size} / $factor, $params{units},
            $self->{warningrange},
            $self->{criticalrange},
            $self->{usable_size} / $factor);
      }
    }
  }
}

# CREATE  REGULAR  TABLESPACE USERSPACE2 PAGESIZE 4 K  MANAGED BY SYSTEM  USING ('/home/db2inst1/db2inst1/NODE0000/TOOLSDB/T0000008', '/opt/ibm/TOOLSDB' ) EXTENTSIZE 16 OVERHEAD 12.67 PREFETCHSIZE 16 TRANSFERRATE 0.18 BUFFERPOOL  IBMDEFAULTBP  DROPPED TABLE RECOVERY OFF
package DBD::DB2::Server::Instance::Database::Bufferpool;

use strict;

our @ISA = qw(DBD::DB2::Server::Instance::Database);


{
  my @bufferpools = ();
  my $initerrors = undef;

  sub add_bufferpool {
    push(@bufferpools, shift);
  }

  sub return_bufferpools {
    return reverse 
        sort { $a->{name} cmp $b->{name} } @bufferpools;
  }
  
  sub init_bufferpools {
    my %params = @_;
    my $num_bufferpools = 0;
    if ($params{mode} =~ /server::instance::database::listbufferpools/) {
      my @bufferpoolresult = $params{handle}->fetchall_array(q{
        SELECT bpname FROM syscat.bufferpools
      });
      foreach (@bufferpoolresult) {
        my ($name, $type) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        my $bufferpool = DBD::DB2::Server::Instance::Database::Bufferpool->new(
            %thisparams);
        add_bufferpool($bufferpool);
        $num_bufferpools++;
      }
      if (! $num_bufferpools) {
        $initerrors = 1;
        return undef;
      }
    } elsif ($params{mode} =~ /server::instance::database::bufferpool::hitratio/) {
      my $sql = q{
        SELECT 
          bp_name,
          pool_data_p_reads, pool_index_p_reads,
          pool_data_l_reads, pool_index_l_reads
        FROM 
          TABLE( snapshot_bp( 'THISDATABASE', -1 ))
        AS 
          snap
        INNER JOIN
          syscat.bufferpools sbp
        ON
          sbp.bpname = snap.bp_name
      };
      $sql =~ s/THISDATABASE/$params{database}/g;
      my @bufferpoolresult = $params{handle}->fetchall_array($sql);
      foreach (@bufferpoolresult) {
        my ($name, $pool_data_p_reads, $pool_index_p_reads,
            $pool_data_l_reads, $pool_index_l_reads) = @{$_};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{pool_data_p_reads} = $pool_data_p_reads;
        $thisparams{pool_index_p_reads} = $pool_index_p_reads;
        $thisparams{pool_data_l_reads} = $pool_data_l_reads;
        $thisparams{pool_index_l_reads} = $pool_index_l_reads;
        my $bufferpool = DBD::DB2::Server::Instance::Database::Bufferpool->new(
            %thisparams);
        add_bufferpool($bufferpool);
        $num_bufferpools++;
      }
      if (! $num_bufferpools) {
        $initerrors = 1;
        return undef;
      }
    }
  }
}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    name => $params{name},
    pool_data_p_reads => $params{pool_data_p_reads},
    pool_index_p_reads => $params{pool_index_p_reads},
    pool_data_l_reads => $params{pool_data_l_reads},
    pool_index_l_reads => $params{pool_index_l_reads},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::database::bufferpool::hitratiodata/) {
    $self->valdiff(\%params, qw(pool_data_l_reads pool_data_p_reads));
    $self->{hitratio} = 
        ($self->{pool_data_l_reads} > 0) ?
        (1 - ($self->{pool_data_p_reads} / $self->{pool_data_l_reads})) * 100
        : 100;
    $self->{hitratio_now} = 
        ($self->{delta_pool_data_l_reads} > 0) ?
        (1 - ($self->{delta_pool_data_p_reads} / $self->{delta_pool_data_l_reads})) * 100
        : 100;
  } elsif ($params{mode} =~ /server::instance::database::bufferpool::hitratioindex/) {
    $self->valdiff(\%params, qw(pool_index_l_reads pool_index_p_reads));
    $self->{hitratio} = 
        ($self->{pool_index_l_reads} > 0) ?
        (1 - ($self->{pool_index_p_reads} / $self->{pool_index_l_reads})) * 100
        : 100;
    $self->{hitratio_now} = 
        ($self->{delta_pool_index_l_reads} > 0) ?
        (1 - ($self->{delta_pool_index_p_reads} / $self->{delta_pool_index_l_reads})) * 100
        : 100;
  } elsif ($params{mode} =~ /server::instance::database::bufferpool::hitratio/) {
    $self->valdiff(\%params, qw(pool_data_l_reads pool_index_l_reads
        pool_data_p_reads pool_index_p_reads));
    $self->{hitratio} = 
        ($self->{pool_data_l_reads} + $self->{pool_index_l_reads}) > 0 ?
        (1 - (($self->{pool_data_p_reads} + $self->{pool_index_p_reads}) /
        ($self->{pool_data_l_reads} + $self->{pool_index_l_reads}))) * 100
        : 100;
    $self->{hitratio_now} = 
        ($self->{delta_pool_data_l_reads} + $self->{delta_pool_index_l_reads}) > 0 ?
        (1 - (($self->{delta_pool_data_p_reads} + $self->{delta_pool_index_p_reads}) /
        ($self->{delta_pool_data_l_reads} + $self->{delta_pool_index_l_reads}))) * 100 
        : 100;
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::database::bufferpool::hitratio(.*)/) {
      my $refkey = 'hitratio'.($params{lookback} ? '_now' : '');
      $self->add_nagios(
          $self->check_thresholds($self->{$refkey}, "98:", "90:"),
              sprintf("bufferpool %s %shitratio is %.2f%%", 
              $self->{name},
              ($1 ? (($1 eq 'data') ? 'data page ' : 'index ') : ''),
              $self->{$refkey})
      );
      $self->add_perfdata(sprintf "\'bp_%s_hitratio\'=%.2f%%;%s;%s",
          lc $self->{name},
          $self->{hitratio},
          $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(sprintf "\'bp_%s_hitratio_now\'=%.2f%%",
          lc $self->{name},
          $self->{hitratio_now});
    }
  }
}


package DBD::DB2::Server::Instance::Database::Partition;

use strict;

our @ISA = qw(DBD::DB2::Server::Instance::Database);


{
  my @partitions = ();
  my $initerrors = undef;

  sub add_partition {
    push(@partitions, shift);
  }

  sub return_partitions {
    return reverse 
        sort { $a->{name} cmp $b->{name} } @partitions;
  }
  
  sub init_partitions {
    my %params = @_;
    my $num_partitions = 0;
    if ($params{mode} =~ /server::instance::database::logutilization/) {
      my @partitionresult = $params{handle}->fetchall_array(q{
        SELECT 
          dbpartitionnum, total_log_used_kb, total_log_available_kb,
          total_log_used_top_kb
        FROM
          sysibmadm.log_utilization
      });
      foreach (@partitionresult) {
        my ($num, $total_log_used_kb, $total_log_available_kb,
            $total_log_used_top_kb) = @{$_};
        my %thisparams = %params;
        $thisparams{num} = $num;
        $thisparams{total_log_used} = $total_log_used_kb * 1024;
        $thisparams{total_log_available} = $total_log_available_kb * 1024;
        $thisparams{total_log_used_top} = $total_log_used_top_kb * 1024;
        my $partition = DBD::DB2::Server::Instance::Database::Partition->new(
            %thisparams);
        add_partition($partition);
        $num_partitions++;
      }
      if (! $num_partitions) {
        $initerrors = 1;
        return undef;
      }
    }
  }
}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    num => $params{num},
    total_log_used => $params{total_log_used},
    total_log_available => $params{total_log_available},
    total_log_used_top => $params{total_log_used_top},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::database::logutilization/) {
    $self->{log_utilization_percent} = $self->{total_log_used} /
        ($self->{total_log_used} + $self->{total_log_available}) * 100;
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::database::logutilization/) {
      $self->add_nagios(
          $self->check_thresholds($self->{log_utilization_percent}, "80", "90"),
              sprintf("log utilization of partition %s is %.2f%%", 
              $self->{num}, $self->{log_utilization_percent}));
      $self->add_perfdata(sprintf "\'part_%s_log_util\'=%.2f%%;%s;%s",
          $self->{num},
          $self->{log_utilization_percent},
          $self->{warningrange}, $self->{criticalrange});
      $self->add_perfdata(sprintf "\'part_%s_log_used\'=%.2fMB",
          lc $self->{num},
          $self->{total_log_used} / 1048576);
      $self->add_perfdata(sprintf "\'part_%s_log_avail\'=%.2fMB",
          lc $self->{num},
          $self->{total_log_available} / 1048576);
      $self->add_perfdata(sprintf "\'part_%s_log_used_top\'=%.2fMB",
          lc $self->{num},
          $self->{total_log_used_top} / 1048576);
    }
  }
}


package DBD::DB2::Server::Instance::Database::Lock;

use strict;

our @ISA = qw(DBD::DB2::Server::Instance::Database);


{
  my @locks = ();
  my $initerrors = undef;

  sub add_lock {
    push(@locks, shift);
  }

  sub return_locks {
    return reverse 
        sort { $a->{name} cmp $b->{name} } @locks;
  }
  
  sub init_locks {
    my %params = @_;
    my $num_locks = 0;
    if (($params{mode} =~ /server::instance::database::lock::deadlocks/) ||
        ($params{mode} =~ /server::instance::database::lock::lockwaits/) ||
        ($params{mode} =~ /server::instance::database::lock::lockwaiting/)) {
      my %thisparams = %params;
      $thisparams{name} = "dummy";
      my $lock = DBD::DB2::Server::Instance::Database::Lock->new(
          %thisparams);
      add_lock($lock);
      $num_locks++;
    }
  }
}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    name => $params{name},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::instance::database::lock::deadlocks/) {
    $self->{deadlocks} = $params{handle}->fetchrow_array(q{
      SELECT deadlocks FROM sysibmadm.snapdb
    });
    if (defined $self->{deadlocks}) {
      $self->valdiff(\%params, qw(deadlocks));
      $self->{deadlocks_per_s} = $self->{delta_deadlocks} / $self->{delta_timestamp};
    } else {
      $self->add_nagios_critical('unable to aquire deadlock information');
    }
  } elsif ($params{mode} =~ /server::instance::database::lock::lockwaits/) {
    $self->{lock_waits} = $params{handle}->fetchrow_array(q{
      SELECT lock_waits FROM sysibmadm.snapdb
    });
    if (defined $self->{lock_waits}) {
      $self->valdiff(\%params, qw(lock_waits));
      $self->{lock_waits_per_s} = $self->{delta_lock_waits} / $self->{delta_timestamp};
    } else {
      $self->add_nagios_critical('unable to aquire lock_waits information');
    }
  } elsif ($params{mode} =~ /server::instance::database::lock::lockwaiting/) {
    ($self->{elapsed_exec_time}, $self->{lock_wait_time}) = $params{handle}->fetchrow_array(q{
      SELECT
          DOUBLE(elapsed_exec_time_s + elapsed_exec_time_ms / 1000000),
          DOUBLE(lock_wait_time / 1000)
      FROM sysibmadm.snapdb
    });
    if (defined $self->{lock_wait_time}) {
      $self->valdiff(\%params, qw(elapsed_exec_time lock_wait_time));
      $self->{lock_wait_time_percent} = $self->{delta_lock_wait_time} ?
          $self->{delta_lock_wait_time} / $self->{delta_elapsed_exec_time} * 100 : 0;
    } else {
      $self->add_nagios_critical('unable to aquire lock_wait_time information');
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::instance::database::lock::deadlocks/) {
      $self->add_nagios(
          $self->check_thresholds($self->{deadlocks_per_s}, 0, 1),
              sprintf("%.6f deadlocs / sec", $self->{deadlocks_per_s}));
      $self->add_perfdata(sprintf "deadlocks_per_sec=%.6f;%s;%s",
          $self->{deadlocks_per_s},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::instance::database::lock::lockwaits/) {
      $self->add_nagios(
          $self->check_thresholds($self->{lock_waits_per_s}, 10, 100),
              sprintf("%.6f lock waits / sec", $self->{lock_waits_per_s}));
      $self->add_perfdata(sprintf "lock_waits_per_sec=%.6f;%s;%s",
          $self->{lock_waits_per_s},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::instance::database::lock::lockwaiting/) {
      $self->add_nagios(
          $self->check_thresholds($self->{lock_wait_time_percent}, 2, 5),
              sprintf("%.6f%% of the time was spent waiting for locks", $self->{lock_wait_time_percent}));
      $self->add_perfdata(sprintf "lock_percent_waiting=%.6f%%;%s;%s",
          $self->{lock_wait_time_percent},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}


package DBD::DB2::Server;

use strict;
use Time::HiRes;
use IO::File;
use File::Copy 'cp';
use Data::Dumper;


{
  our $verbose = 0;
  our $scream = 0; # scream if something is not implemented
  our $my_modules_dyn_dir = ""; # where we look for self-written extensions

  my @servers = ();
  my $initerrors = undef;

  sub add_server {
    push(@servers, shift);
  }

  sub return_servers {
    return @servers;
  }
  
  sub return_first_server() {
    return $servers[0];
  }

}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    method => $params{method} || "dbi",
    hostname => $params{hostname},
    database => $params{database},
    username => $params{username},
    password => $params{password},
    port => $params{port} || 50000,
    timeout => $params{timeout},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    verbose => $params{verbose},
    report => $params{report},
    version => 'unknown',
    os => 'unknown',
    instance => undef,
    databases => [],
    handle => undef,
  };
  bless $self, $class;
  $self->init_nagios();
  if ($self->dbconnect(%params)) {
    $self->{version} = $self->{handle}->fetchrow_array(
        q{ SELECT prod_release FROM sysibmadm.env_prod_info });
    #$self->{os} = $self->{handle}->fetchrow_array(
    #    q{ SELECT dbms_utility.port_string FROM dual });
    $self->{dbuser} = $self->{handle}->fetchrow_array(
        q{ SELECT current_user FROM sysibm.sysdummy1 });
    #$self->{thread} = $self->{handle}->fetchrow_array(
    #    q{ SELECT thread# FROM v$instance });
    #$self->{parallel} = $self->{handle}->fetchrow_array(
    #    q{ SELECT parallel FROM v$instance });
    DBD::DB2::Server::add_server($self);
    $self->init(%params);
  }
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $params{handle} = $self->{handle};
  $self->set_global_db_thresholds(\%params);
  if ($params{mode} =~ /^server::instance/) {
    $self->{instance} = DBD::DB2::Server::Instance->new(%params);
  } elsif ($params{mode} =~ /^server::database/) {
    $self->{database} = DBD::DB2::Server::Database->new(%params);
  } elsif ($params{mode} =~ /^server::sql/) {
    $self->set_local_db_thresholds(%params);
    if ($params{name2} && $params{name2} ne $params{name}) {
      $self->{genericsql} =
          $self->{handle}->fetchrow_array($params{selectname});
      if (! defined $self->{genericsql}) {
        $self->add_nagios_unknown(sprintf "got no valid response for %s",
            $params{selectname});
      }
    } else {
      @{$self->{genericsql}} =
          $self->{handle}->fetchrow_array($params{selectname});
      if (! (defined $self->{genericsql} &&
          (scalar(grep { /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/ } @{$self->{genericsql}})) ==
          scalar(@{$self->{genericsql}}))) {
        $self->add_nagios_unknown(sprintf "got no valid response for %s",
            $params{selectname});
      } else {
        # name2 in array
        # units in array
      }
    }
  } elsif ($params{mode} =~ /^server::connectiontime/) {
    $self->{connection_time} = $self->{tac} - $self->{tic};
  } elsif ($params{mode} =~ /^my::([^:.]+)/) {
    my $class = $1;
    my $loaderror = undef;
    substr($class, 0, 1) = uc substr($class, 0, 1);
    foreach my $libpath (split(":", $DBD::DB2::Server::my_modules_dyn_dir)) {
      foreach my $extmod (glob $libpath."/CheckDB2Health*.pm") {
        eval {
          $self->trace(sprintf "loading module %s", $extmod);
          require $extmod;
        };
        if ($@) {
          $loaderror = $extmod;
          $self->trace(sprintf "failed loading module %s: %s", $extmod, $@);
        }
      }
    }
    my $obj = {
        handle => $params{handle},
        warningrange => $params{warningrange},
        criticalrange => $params{criticalrange},
    };
    bless $obj, "My$class";
    $self->{my} = $obj;
    if ($self->{my}->isa("DBD::DB2::Server")) {
      my $dos_init = $self->can("init");
      my $dos_nagios = $self->can("nagios");
      my $my_init = $self->{my}->can("init");
      my $my_nagios = $self->{my}->can("nagios");
      if ($my_init == $dos_init) {
          $self->add_nagios_unknown(
              sprintf "Class %s needs an init() method", ref($self->{my}));
      } elsif ($my_nagios == $dos_nagios) {
          $self->add_nagios_unknown(
              sprintf "Class %s needs a nagios() method", ref($self->{my}));
      } else {
        $self->{my}->init_nagios(%params);
        $self->{my}->init(%params);
      }
    } else {
      $self->add_nagios_unknown(
          sprintf "Class %s is not a subclass of DBD::DB2::Server%s", 
              ref($self->{my}),
              $loaderror ? sprintf " (syntax error in %s?)", $loaderror : "" );
    }
  } else {
    printf "broken mode %s\n", $params{mode};
  }
}

sub dump {
  my $self = shift;
  my $message = shift || "";
  printf "%s %s\n", $message, Data::Dumper::Dumper($self);
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /^server::instance/) {
      $self->{instance}->nagios(%params);
      $self->merge_nagios($self->{instance});
    } elsif ($params{mode} =~ /^server::database/) {
      $self->{database}->nagios(%params);
      $self->merge_nagios($self->{database});
    } elsif ($params{mode} =~ /^server::connectiontime/) {
      $self->add_nagios(
          $self->check_thresholds($self->{connection_time}, 1, 5),
          sprintf "%.2f seconds to connect as %s",
              $self->{connection_time}, $self->{dbuser});
      $self->add_perfdata(sprintf "connection_time=%.4f;%d;%d",
          $self->{connection_time},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::sql/) {
      if ($params{name2} && $params{name2} ne $params{name}) {
        if ($params{regexp}) {
          if ($self->{genericsql} =~ /$params{name2}/) {
            $self->add_nagios_ok(
                sprintf "output %s matches pattern %s",
                    $self->{genericsql}, $params{name2});
          } else {
            $self->add_nagios_critical(
                sprintf "output %s does not match pattern %s",
                    $self->{genericsql}, $params{name2});
          }
        } else {
          if ($self->{genericsql} eq $params{name2}) {
            $self->add_nagios_ok(
                sprintf "output %s found", $self->{genericsql});
          } else {
            $self->add_nagios_critical(
                sprintf "output %s not found", $self->{genericsql});
          }
        }
      } else {
        $self->add_nagios(
            # the first item in the list will trigger the threshold values
            $self->check_thresholds($self->{genericsql}[0], 1, 5),
                sprintf "%s: %s%s",
                $params{name2} ? lc $params{name2} : lc $params{selectname},
                # float as float, integers as integers
                join(" ", map {
                    (sprintf("%d", $_) eq $_) ? $_ : sprintf("%f", $_)
                } @{$self->{genericsql}}),
                $params{units} ? $params{units} : "");
        my $i = 0;
        # workaround... getting the column names from the database would be nicer
        my @names2_arr = split(/\s+/, $params{name2});
        foreach my $t (@{$self->{genericsql}}) {
          $self->add_perfdata(sprintf "\'%s\'=%s%s;%s;%s",
              $names2_arr[$i] ? lc $names2_arr[$i] : lc $params{selectname},
              # float as float, integers as integers
              (sprintf("%d", $t) eq $t) ? $t : sprintf("%f", $t),
              $params{units} ? $params{units} : "",
            ($i == 0) ? $self->{warningrange} : "",
              ($i == 0) ? $self->{criticalrange} : ""
          );
          $i++;
        }
      }
    } elsif ($params{mode} =~ /^my::([^:.]+)/) {
      $self->{my}->nagios(%params);
      $self->merge_nagios($self->{my});
    }
  }
}


sub init_nagios {
  my $self = shift;
  no strict 'refs';
  if (! ref($self)) {
    my $nagiosvar = $self."::nagios";
    my $nagioslevelvar = $self."::nagios_level";
    $$nagiosvar = {
      messages => {
        0 => [],
        1 => [],
        2 => [],
        3 => [],
      },
      perfdata => [],
    };
    $$nagioslevelvar = $ERRORS{OK},
  } else {
    $self->{nagios} = {
      messages => {
        0 => [],
        1 => [],
        2 => [],
        3 => [],
      },
      perfdata => [],
    };
    $self->{nagios_level} = $ERRORS{OK},
  }
}

sub check_thresholds {
  my $self = shift;
  my $value = shift;
  my $defaultwarningrange = shift;
  my $defaultcriticalrange = shift;
  my $level = $ERRORS{OK};
  $self->{warningrange} = defined $self->{warningrange} ?
      $self->{warningrange} : $defaultwarningrange;
  $self->{criticalrange} = defined $self->{criticalrange} ?
      $self->{criticalrange} : $defaultcriticalrange;
  if ($self->{warningrange} !~ /:/ && $self->{criticalrange} !~ /:/) {
    # warning = 10, critical = 20, warn if > 10, crit if > 20
    $level = $ERRORS{WARNING} if $value > $self->{warningrange};
    $level = $ERRORS{CRITICAL} if $value > $self->{criticalrange};
  } elsif ($self->{warningrange} =~ /(\d+):/ && 
      $self->{criticalrange} =~ /(\d+):/) {
    # warning = 98:, critical = 95:, warn if < 98, crit if < 95
    $self->{warningrange} =~ /(\d+):/;
    $level = $ERRORS{WARNING} if $value < $1;
    $self->{criticalrange} =~ /(\d+):/;
    $level = $ERRORS{CRITICAL} if $value < $1;
  }
  return $level;
  #
  # syntax error must be reported with returncode -1
  #
}

sub add_nagios {
  my $self = shift;
  my $level = shift;
  my $message = shift;
  push(@{$self->{nagios}->{messages}->{$level}}, $message);
  # recalc current level
  foreach my $llevel qw(CRITICAL WARNING UNKNOWN OK) {
    if (scalar(@{$self->{nagios}->{messages}->{$ERRORS{$llevel}}})) {
      $self->{nagios_level} = $ERRORS{$llevel};
    }
  }
}

sub add_nagios_ok {
  my $self = shift;
  my $message = shift;
  $self->add_nagios($ERRORS{OK}, $message);
}

sub add_nagios_warning {
  my $self = shift;
  my $message = shift;
  $self->add_nagios($ERRORS{WARNING}, $message);
}

sub add_nagios_critical {
  my $self = shift;
  my $message = shift;
  $self->add_nagios($ERRORS{CRITICAL}, $message);
}

sub add_nagios_unknown {
  my $self = shift;
  my $message = shift;
  $self->add_nagios($ERRORS{UNKNOWN}, $message);
}

sub add_perfdata {
  my $self = shift;
  my $data = shift;
  push(@{$self->{nagios}->{perfdata}}, $data);
}

sub merge_nagios {
  my $self = shift;
  my $child = shift;
  foreach my $level (0..3) {
    foreach (@{$child->{nagios}->{messages}->{$level}}) {
      $self->add_nagios($level, $_);
    }
    #push(@{$self->{nagios}->{messages}->{$level}},
    #    @{$child->{nagios}->{messages}->{$level}});
  }
  push(@{$self->{nagios}->{perfdata}}, @{$child->{nagios}->{perfdata}});
}

sub calculate_result {
  my $self = shift;
  my $multiline = 0;
  map {
    $self->{nagios_level} = $ERRORS{$_} if
        (scalar(@{$self->{nagios}->{messages}->{$ERRORS{$_}}}));
  } ("OK", "UNKNOWN", "WARNING", "CRITICAL");
  if ($ENV{NRPE_MULTILINESUPPORT} &&
      length join(" ", @{$self->{nagios}->{perfdata}}) > 200) {
    $multiline = 1;
  }
  my $all_messages = join(($multiline ? "\n" : ", "), map {
      join(($multiline ? "\n" : ", "), @{$self->{nagios}->{messages}->{$ERRORS{$_}}})
  } grep {
      scalar(@{$self->{nagios}->{messages}->{$ERRORS{$_}}})
  } ("CRITICAL", "WARNING", "UNKNOWN", "OK"));
  my $bad_messages = join(($multiline ? "\n" : ", "), map {
      join(($multiline ? "\n" : ", "), @{$self->{nagios}->{messages}->{$ERRORS{$_}}})
  } grep {
      scalar(@{$self->{nagios}->{messages}->{$ERRORS{$_}}})
  } ("CRITICAL", "WARNING", "UNKNOWN"));
  my $all_messages_short = $bad_messages ? $bad_messages : 'no problems';
  my $all_messages_html = "<table style=\"border-collapse: collapse;\">".
      join("", map {
          my $level = $_;
          join("", map {
              sprintf "<tr valign=\"top\"><td class=\"service%s\">%s</td></tr>",
              $level, $_;
          } @{$self->{nagios}->{messages}->{$ERRORS{$_}}});
      } grep {
          scalar(@{$self->{nagios}->{messages}->{$ERRORS{$_}}})
      } ("CRITICAL", "WARNING", "UNKNOWN", "OK")).
  "</table>";
  if (exists $self->{identstring}) {
    $self->{nagios_message} .= $self->{identstring};
  }
  if ($self->{report} eq "long") {
    $self->{nagios_message} .= $all_messages;
  } elsif ($self->{report} eq "short") {
    $self->{nagios_message} .= $all_messages_short;
  } elsif ($self->{report} eq "html") {
    $self->{nagios_message} .= $all_messages_short."\n".$all_messages_html;
  }
  $self->{perfdata} = join(" ", @{$self->{nagios}->{perfdata}});
}

sub set_global_db_thresholds {
  my $self = shift;
  my $params = shift;
  my $warning = undef;
  my $critical = undef;
  return unless defined $params->{dbthresholds};
  $params->{name0} = $params->{dbthresholds};
  # :pluginmode   :name     :warning    :critical
  # mode          empty     
  # 
  eval {
    if ($self->{handle}->fetchrow_array(q{
        SELECT tabname FROM syscat.tables
        WHERE tabname = 'CHECK_DB2_HEALTH_THRESHOLDS'
      })) {
      my @dbthresholds = $self->{handle}->fetchall_array(q{
          SELECT * FROM check_db2_health_thresholds
      });
      $params->{dbthresholds} = \@dbthresholds;
      foreach (@dbthresholds) { 
        if (($_->[0] eq $params->{cmdlinemode}) &&
            (! defined $_->[1] || ! $_->[1])) { 
          ($warning, $critical) = ($_->[2], $_->[3]);
        }
      }
    }
  };
  if (! $@) {
    if ($warning) {
      $params->{warningrange} = $warning;
      $self->trace("read warningthreshold %s from database", $warning);
    }
    if ($critical) {
      $params->{criticalrange} = $critical;
      $self->trace("read criticalthreshold %s from database", $critical);
    }
  }
}

sub set_local_db_thresholds {
  my $self = shift;
  my %params = @_;
  my $warning = undef;
  my $critical = undef;
  # :pluginmode   :name     :warning    :critical
  # mode          name0
  # mode          name2
  # mode          name
  #
  # first: argument of --dbthresholds, it it exists
  # second: --name2
  # third: --name
  if (ref($params{dbthresholds}) eq 'ARRAY') {
    my $marker;
    foreach (@{$params{dbthresholds}}) {
      if ($_->[0] eq $params{cmdlinemode}) {
        if (defined $_->[1] && $params{name0} && $_->[1] eq $params{name0}) {
          ($warning, $critical) = ($_->[2], $_->[3]);
          $marker = $params{name0};
          last;
        } elsif (defined $_->[1] && $params{name2} && $_->[1] eq $params{name2}) {
          ($warning, $critical) = ($_->[2], $_->[3]);
          $marker = $params{name2};
          last;
        } elsif (defined $_->[1] && $params{name} && $_->[1] eq $params{name}) {
          ($warning, $critical) = ($_->[2], $_->[3]);
          $marker = $params{name};
          last;
        }
      }
    }
    if ($warning) {
      $self->{warningrange} = $warning;
      $self->trace("read warningthreshold %s for %s from database",
         $marker, $warning);
    }
    if ($critical) {
      $self->{criticalrange} = $critical;
      $self->trace("read criticalthreshold %s for %s from database",
          $marker, $critical);
    }
  }
}

sub debug {
  my $self = shift;
  my $msg = shift;
  if ($DBD::DB2::Server::verbose) {
    printf "%s %s\n", $msg, ref($self);
  }
}

sub dbconnect {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  $self->{tic} = Time::HiRes::time();
  $self->{handle} = DBD::DB2::Server::Connection->new(%params);
  if ($self->{handle}->{errstr}) {
    if ($self->{handle}->{errstr} eq "alarm\n") {
      $self->add_nagios($ERRORS{CRITICAL},
          sprintf "connection could not be established within %d seconds",
              $self->{timeout});
    } else {
      $self->add_nagios($ERRORS{CRITICAL},
          sprintf "cannot connect to %s. %s",
          ($self->{hostname} || $self->{database}), $self->{handle}->{errstr});
      $retval = undef;
    }
  } else {
    $retval = $self->{handle};
  }
  $self->{tac} = Time::HiRes::time();
  return $retval;
}

sub trace {
  my $self = shift;
  my $format = shift;
  $self->{trace} = -f "/tmp/check_db2_health.trace" ? 1 : 0;
  if ($self->{verbose}) {
    printf("%s: ", scalar localtime);
    printf($format, @_);
  }
  if ($self->{trace}) {
    my $logfh = new IO::File;
    $logfh->autoflush(1);
    if ($logfh->open("/tmp/check_db2_health.trace", "a")) {
      $logfh->printf("%s: ", scalar localtime);
      $logfh->printf($format, @_);
      $logfh->printf("\n");
      $logfh->close();
    }
  }
}

sub DESTROY {
  my $self = shift;
  my $handle1 = "null";
  my $handle2 = "null";
  if (defined $self->{handle}) {
    $handle1 = ref($self->{handle});
    if (defined $self->{handle}->{handle}) {
      $handle2 = ref($self->{handle}->{handle});
    }
  }
  $self->trace(sprintf "DESTROY %s with handle %s %s", ref($self), $handle1, $handle2);
  if (ref($self) eq "DBD::DB2::Server") {
  }
  $self->trace(sprintf "DESTROY %s exit with handle %s %s", ref($self), $handle1, $handle2);
  if (ref($self) eq "DBD::DB2::Server") {
    #printf "humpftata\n";
  }
}

sub save_state {
  my $self = shift;
  my %params = @_;
  my $extension = "";
  mkdir $params{statefilesdir} unless -d $params{statefilesdir};
  my $statefile = sprintf "%s/%s", 
      $params{statefilesdir}, $params{mode};
  $extension .= $params{differenciator} ? "_".$params{differenciator} : "";
  $extension .= $params{database} ? "_".$params{database} : "";
  $extension .= $params{hostname} ? "_".$params{hostname} : "";
  $extension .= $params{tablespace} ? "_".$params{tablespace} : "";
  $extension .= $params{datafile} ? "_".$params{datafile} : "";
  $extension .= $params{name} ? "_".$params{name} : "";
  $extension =~ s/\//_/g;
  $extension =~ s/\(/_/g;
  $extension =~ s/\)/_/g;
  $extension =~ s/\*/_/g;
  $extension =~ s/\s/_/g;
  $statefile .= $extension;
  $statefile = lc $statefile;
  open(STATE, ">$statefile");
  if ((ref($params{save}) eq "HASH") && exists $params{save}->{timestamp}) {
    $params{save}->{localtime} = scalar localtime $params{save}->{timestamp};
  }
  printf STATE Data::Dumper::Dumper($params{save});
  close STATE;
  $self->debug(sprintf "saved %s to %s",
      Data::Dumper::Dumper($params{save}), $statefile);
}

sub load_state {
  my $self = shift;
  my %params = @_;
  my $extension = "";
  my $statefile = sprintf "%s/%s", 
      $params{statefilesdir}, $params{mode};
  $extension .= $params{differenciator} ? "_".$params{differenciator} : "";
  $extension .= $params{database} ? "_".$params{database} : "";
  $extension .= $params{hostname} ? "_".$params{hostname} : "";
  $extension .= $params{tablespace} ? "_".$params{tablespace} : "";
  $extension .= $params{datafile} ? "_".$params{datafile} : "";
  $extension .= $params{name} ? "_".$params{name} : "";
  $extension =~ s/\//_/g;
  $extension =~ s/\(/_/g;
  $extension =~ s/\)/_/g;
  $extension =~ s/\*/_/g;
  $extension =~ s/\s/_/g;
  $statefile .= $extension;
  $statefile = lc $statefile;
  if ( -f $statefile) {
    our $VAR1;
    eval {
      require $statefile;
    };
    if($@) {
printf "rumms\n";
    }
    $self->debug(sprintf "load %s", Data::Dumper::Dumper($VAR1));
    return $VAR1;
  } else {
    return undef;
  }
}

sub valdiff {
  my $self = shift;
  my $pparams = shift;
  my %params = %{$pparams};
  my @keys = @_;
  my $now = time;
  my $last_values = $self->load_state(%params) || eval {
    my $empty_events = {};
    foreach (@keys) {
      $empty_events->{$_} = 0;
    }
    $empty_events->{timestamp} = 0;
    if ($params{lookback}) {
      $empty_events->{lookback_history} = {};
    }
    $empty_events;
  };
  foreach (@keys) {
    if ($params{lookback}) {
      # find a last_value in the history which fits lookback best
      # and overwrite $last_values->{$_} with historic data
      if (exists $last_values->{lookback_history}->{$_}) {
        foreach my $date (sort {$a <=> $b} keys %{$last_values->{lookback_history}->{$_}}) {
          if ($date >= ($now - $params{lookback})) {
            $last_values->{$_} = $last_values->{lookback_history}->{$_}->{$date};
            $last_values->{timestamp} = $date;
            last;
          } else {
            delete $last_values->{lookback_history}->{$_}->{$date};
          }
        }
      }
    }
    $last_values->{$_} = 0 if ! exists $last_values->{$_};
    if ($self->{$_} >= $last_values->{$_}) {
      $self->{'delta_'.$_} = $self->{$_} - $last_values->{$_};
    } else {
      # vermutlich db restart und zaehler alle auf null
      $self->{'delta_'.$_} = $self->{$_};
    }
    $self->debug(sprintf "delta_%s %f", $_, $self->{'delta_'.$_});
  }
  $self->{'delta_timestamp'} = $now - $last_values->{timestamp};
  $params{save} = eval {
    my $empty_events = {};
    foreach (@keys) {
      $empty_events->{$_} = $self->{$_};
    }
    $empty_events->{timestamp} = $now;
    if ($params{lookback}) {
      $empty_events->{lookback_history} = $last_values->{lookback_history};
      foreach (@keys) {
        $empty_events->{lookback_history}->{$_}->{$now} = $self->{$_};
      }
    }
    $empty_events;
  };
  $self->save_state(%params);
}

sub requires_version {
  my $self = shift;
  my $version = shift;
  my @instances = DBD::DB2::Server::return_servers();
  my $instversion = $instances[0]->{version};
  if (! $self->version_is_minimum($version)) {
    $self->add_nagios($ERRORS{UNKNOWN}, 
        sprintf "not implemented/possible for DB2 release %s", $instversion);
  }
}

sub version_is_minimum {
  # the current version is newer or equal
  my $self = shift;
  my $version = shift;
  my $newer = 1;
  my @instances = DBD::DB2::Server::return_servers();
  my @v1 = map { $_ eq "x" ? 0 : $_ } split(/\./, $version);
  my @v2 = split(/\./, $instances[0]->{version});
  if (scalar(@v1) > scalar(@v2)) {
    push(@v2, (0) x (scalar(@v1) - scalar(@v2)));
  } elsif (scalar(@v2) > scalar(@v1)) {
    push(@v1, (0) x (scalar(@v2) - scalar(@v1)));
  }
  foreach my $pos (0..$#v1) {
    if ($v2[$pos] > $v1[$pos]) {
      $newer = 1;
      last;
    } elsif ($v2[$pos] < $v1[$pos]) {
      $newer = 0;
      last;
    }
  }
  #printf STDERR "check if %s os minimum %s\n", join(".", @v2), join(".", @v1);
  return $newer;
}

sub instance_rac {
  my $self = shift;
  my @instances = DBD::DB2::Server::return_servers();
  return (lc $instances[0]->{parallel} eq "yes") ? 1 : 0;
}

sub instance_thread {
  my $self = shift;
  my @instances = DBD::DB2::Server::return_servers();
  return $instances[0]->{thread};
}

sub windows_server {
  my $self = shift;
  my @instances = DBD::DB2::Server::return_servers();
  if ($instances[0]->{os} =~ /Win/i) {
    return 1;
  } else {
    return 0;
  }
}

sub system_vartmpdir {
  my $self = shift;
  if ($^O =~ /MSWin/) {
    return $self->system_tmpdir();
  } else {
    return "/var/tmp/check_db2_health";
  }
}

sub system_oldvartmpdir {
  my $self = shift;
  return "/tmp";
}

sub system_tmpdir {
  my $self = shift;
  if ($^O =~ /MSWin/) {
    return $ENV{TEMP} if defined $ENV{TEMP};
    return $ENV{TMP} if defined $ENV{TMP};
    return File::Spec->catfile($ENV{windir}, 'Temp')
        if defined $ENV{windir};
    return 'C:\Temp';
  } else {
    return "/tmp";
  }
}


package DBD::DB2::Server::Connection;

use strict;

our @ISA = qw(DBD::DB2::Server);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    mode => $params{mode},
    timeout => $params{timeout},
    method => $params{method} || "dbi",
    hostname => $params{hostname},
    database => $params{database},
    username => $params{username},
    password => $params{password},
    port => $params{port} || 50000,
    db2home => $ENV{DB2_HOME},
    handle => undef,
  };
  bless $self, $class;
  if ($params{method} eq "dbi") {
    bless $self, "DBD::DB2::Server::Connection::Dbi";
  } elsif ($params{method} eq "sqlrelay") {
    bless $self, "DBD::DB2::Server::Connection::Sqlrelay";
  }
  $self->init(%params);
  return $self;
}


package DBD::DB2::Server::Connection::Dbi;

use strict;
use Net::Ping;

our @ISA = qw(DBD::DB2::Server::Connection);


sub init {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  if ($self->{mode} =~ /^server::tnsping/) {
    # keine ahnung ob das bei db2 zu gebrauchen ist
    if (! $self->{connect}) {
      $self->{errstr} = "Please specify a database";
    } else {
      $self->{sid} = $self->{connect};
      $self->{user} ||= time;  # prefer an existing user
      $self->{password} = time;
    }
  } else {
    # entweder katalogisiert oder nicht
    # database username password
    # database username password hostname port
    if (! $self->{database} || ! $self->{username} || ! $self->{password}) {
      $self->{errstr} = "Please specify database, username and password";
      return undef;
    }
    $self->{dsn} = "DBI:DB2:";
    if (! $self->{hostname}) {
      # catalog tcpip node <host-nickname> remote <hostname> server <port>
      # catalog database <remote-db> as <local-nick> at node <host-nickname>
      $self->{dsn} .= $self->{database};
    } else {
      $self->{dsn} .= sprintf "DATABASE=%s; ", $self->{database};
      $self->{dsn} .= sprintf "HOSTNAME=%s; ", $self->{hostname};
      $self->{dsn} .= sprintf "PORT=%d; ", $self->{port};
      $self->{dsn} .= sprintf "PROTOCOL=TCPIP; ";
      $self->{dsn} .= sprintf "UID=%s; ", $self->{username};
      $self->{dsn} .= sprintf "PWD=%s;", $self->{password};
    }
  }
  if (! exists $self->{errstr}) {
    eval {
      require DBI;
      use POSIX ':signal_h';
      local $SIG{'ALRM'} = sub {
        die "alarm\n";
      };
      my $mask = POSIX::SigSet->new( SIGALRM );
      my $action = POSIX::SigAction->new(
          sub { die "alarm\n" ; }, $mask);
      my $oldaction = POSIX::SigAction->new();
      sigaction(SIGALRM ,$action ,$oldaction );
      alarm($self->{timeout} - 1); # 1 second before the global unknown timeout
      if ($self->{handle} = DBI->connect(
          $self->{dsn},
          $self->{username},
          $self->{password},
          { RaiseError => 0, AutoCommit => 1, PrintError => 0 })) {
        $retval = $self;
      } else {
        $self->{errstr} = DBI::errstr();
      }
    };
    if ($@) {
      $self->{errstr} = $@;
      $retval = undef;
    }
  }
  $self->{tac} = Time::HiRes::time();
  return $retval;
}

sub fetchrow_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my @row = ();
  eval {
    $self->trace(sprintf "SQL:\n%s\n", $sql);
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $self->trace(sprintf "ARGS:\n%s\n", Data::Dumper::Dumper(\@arguments));
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    @row = $sth->fetchrow_array();
    $self->trace(sprintf "RESULT:\n%s\n", Data::Dumper::Dumper(\@row));
  }; 
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
  }
  if (-f "/tmp/check_db2_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) = 
        "/tmp/check_db2_health_simulation/".$self->{mode}; <> };
    @row = split(/\s+/, (split(/\n/, $simulation))[0]);
  }
  return $row[0] unless wantarray;
  return @row;
}

sub fetchall_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my $rows = undef;
  eval {
    $self->trace(sprintf "SQL:\n%s\n", $sql);
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $self->trace(sprintf "ARGS:\n%s\n", Data::Dumper::Dumper(\@arguments));
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    $rows = $sth->fetchall_arrayref();
    $self->trace(sprintf "RESULT:\n%s\n", Data::Dumper::Dumper($rows));
  }; 
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
  if (-f "/tmp/check_db2_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) = 
        "/tmp/check_db2_health_simulation/".$self->{mode}; <> };
    @{$rows} = map { [ split(/\s+/, $_) ] } split(/\n/, $simulation);
  }
  return @{$rows};
}

sub fetchall_hashref {
  my $self = shift;
  my $sql = shift;
  my $hashkey = shift;
  my @arguments = @_;
  my $sth = undef;
  my $hashref = undef;
  eval {
DBI->trace(3);

    $sth = $self->{handle}->prepare($sql);
$sth->{db2_concurrency} = 'SQL_CONCUR_READ_ONLY';
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
while (my $orks = $sth->fetchrow_arrayref()) {
printf "doof: %s\n", Data::Dumper::Dumper($orks);
}
    $hashref = $sth->fetchall_hashref($hashkey);
  };
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
  return $hashref;
}


sub func {
  my $self = shift;
  $self->{handle}->func(@_);
}


sub execute {
  my $self = shift;
  my $sql = shift;
  eval {
    my $sth = $self->{handle}->prepare($sql);
    $sth->execute();
  };
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
}

sub DESTROY {
  my $self = shift;
  $self->trace(sprintf "disconnecting DBD %s",
      $self->{handle} ? "with handle" : "without handle");
  $self->{handle}->disconnect() if $self->{handle};
}


package DBD::DB2::Server::Connection::Sqlrelay;

use strict;
use Net::Ping;

our @ISA = qw(DBD::DB2::Server::Connection);


sub init {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  if ($self->{mode} =~ /^server::tnsping/) {
    if (! $self->{connect}) {
      $self->{errstr} = "Please specify a database";
    } else {
      if ($self->{connect} =~ /([\.\w]+):(\d+)/) {
        $self->{host} = $1;
        $self->{port} = $2;
        $self->{socket} = "";
      } elsif ($self->{connect} =~ /([\.\w]+):([\w\/]+)/) {
        $self->{host} = $1;
        $self->{socket} = $2;
        $self->{port} = "";
      }
    }
  } else {
    if (! $self->{connect} || ! $self->{user} || ! $self->{password}) {
      if ($self->{connect} && $self->{connect} =~ /(\w+)\/(\w+)@([\.\w]+):(\d+)/) {
        $self->{user} = $1;
        $self->{password} = $2;
        $self->{host} = $3; 
        $self->{port} = $4;
        $self->{socket} = "";
      } elsif ($self->{connect} && $self->{connect} =~ /(\w+)\/(\w+)@([\.\w]+):([\w\/]+)/) {
        $self->{user} = $1;
        $self->{password} = $2;
        $self->{host} = $3; 
        $self->{socket} = $4;
        $self->{port} = "";
      } else {
        $self->{errstr} = "Please specify database, username and password";
        return undef;
      }
    } else {
      if ($self->{connect} =~ /([\.\w]+):(\d+)/) {
        $self->{host} = $1;
        $self->{port} = $2;
        $self->{socket} = "";
      } elsif ($self->{connect} =~ /([\.\w]+):([\w\/]+)/) {
        $self->{host} = $1;
        $self->{socket} = $2;
        $self->{port} = "";
      } else {
        $self->{errstr} = "Please specify database, username and password";
        return undef;
      }
    }
  }
  if (! exists $self->{errstr}) {
    eval {
      require DBI;
      use POSIX ':signal_h';
      local $SIG{'ALRM'} = sub {
        die "alarm\n";
      };
      my $mask = POSIX::SigSet->new( SIGALRM );
      my $action = POSIX::SigAction->new(
      sub { die "alarm\n" ; }, $mask);
      my $oldaction = POSIX::SigAction->new();
      sigaction(SIGALRM ,$action ,$oldaction );
      alarm($self->{timeout} - 1); # 1 second before the global unknown timeout
      if ($self->{handle} = DBI->connect(
          sprintf("DBI:SQLRelay:host=%s;port=%d;socket=%s", $self->{host}, $self->{port}, $self->{socket}),
          $self->{user},
          $self->{password},
          { RaiseError => 1, AutoCommit => 0, PrintError => 1 })) {
        $self->{handle}->do(q{
            ALTER SESSION SET NLS_NUMERIC_CHARACTERS=".," });
        $retval = $self;
        if ($self->{mode} =~ /^server::tnsping/ && $self->{handle}->ping()) {
          # database connected. fake a "unknown user"
          $self->{errstr} = "ERR: unknown user";
        }
      } else {
        $self->{errstr} = DBI::errstr();
      }
    };
    if ($@) {
      $self->{errstr} = $@;
      $self->{errstr} =~ s/at [\w\/\.]+ line \d+.*//g;
      $retval = undef;
    }
  }
  $self->{tac} = Time::HiRes::time();
  return $retval;
}

sub fetchrow_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my @row = ();
  $self->trace(sprintf "fetchrow_array: %s", $sql);
  eval {
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    @row = $sth->fetchrow_array();
  };
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
  }
  if (-f "/tmp/check_db2_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) =
        "/tmp/check_db2_health_simulation/".$self->{mode}; <> };
    @row = split(/\s+/, (split(/\n/, $simulation))[0]);
  }
  return $row[0] unless wantarray;
  return @row;
}

sub fetchall_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my $rows = undef;
  $self->trace(sprintf "fetchall_array: %s", $sql);
  eval {
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    $rows = $sth->fetchall_arrayref();
  };
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
  if (-f "/tmp/check_db2_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) =
        "/tmp/check_db2_health_simulation/".$self->{mode}; <> };
    @{$rows} = map { [ split(/\s+/, $_) ] } split(/\n/, $simulation);
  }
  return @{$rows};
}

sub func {
  my $self = shift;
  $self->{handle}->func(@_);
}


sub execute {
  my $self = shift;
  my $sql = shift;
  eval {
    my $sth = $self->{handle}->prepare($sql);
    $sth->execute();
  };
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
}

sub DESTROY {
  my $self = shift;
  #$self->trace(sprintf "disconnecting DBD %s",
  #    $self->{handle} ? "with handle" : "without handle");
  #$self->{handle}->disconnect() if $self->{handle};
}





package Extraopts;

use strict;
use File::Basename;
use Data::Dumper;

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    file => $params{file},
    commandline => $params{commandline},
    config => {},
    section => 'default_no_section',
  };
  bless $self, $class;
  $self->prepare_file_and_section();
  $self->init();
  return $self;
}

sub prepare_file_and_section {
  my $self = shift;
  if (! defined $self->{file}) {
    # ./check_stuff --extra-opts
    $self->{section} = basename($0);
    $self->{file} = $self->get_default_file();
  } elsif ($self->{file} =~ /^[^@]+$/) {
    # ./check_stuff --extra-opts=special_opts
    $self->{section} = $self->{file};
    $self->{file} = $self->get_default_file();
  } elsif ($self->{file} =~ /^@(.*)/) {
    # ./check_stuff --extra-opts=@/etc/myconfig.ini
    $self->{section} = basename($0);
    $self->{file} = $1;
  } elsif ($self->{file} =~ /^(.*?)@(.*)/) {
    # ./check_stuff --extra-opts=special_opts@/etc/myconfig.ini
    $self->{section} = $1;
    $self->{file} = $2;
  }
}

sub get_default_file {
  my $self = shift;
  foreach my $default (qw(/etc/nagios/plugins.ini
      /usr/local/nagios/etc/plugins.ini
      /usr/local/etc/nagios/plugins.ini
      /etc/opt/nagios/plugins.ini
      /etc/nagios-plugins.ini
      /usr/local/etc/nagios-plugins.ini
      /etc/opt/nagios-plugins.ini)) {
    if (-f $default) {
      return $default;
    }
  }
  return undef;
}

sub init {
  my $self = shift;
  if (! defined $self->{file}) {
    $self->{errors} = sprintf 'no extra-opts file specified and no default file found';
  } elsif (! -f $self->{file}) {
    $self->{errors} = sprintf 'could not open %s', $self->{file};
  } else {
    my $data = do { local (@ARGV, $/) = $self->{file}; <> };
    my $in_section = 'default_no_section';
    foreach my $line (split(/\n/, $data)) {
      if ($line =~ /\[(.*)\]/) {
        $in_section = $1;
      } elsif ($line =~ /(.*)=(.*)/) {
        $self->{config}->{$in_section}->{$1} = $2;
      }
    }
  }
}

sub is_valid {
  my $self = shift;
  return ! exists $self->{errors};
}

sub overwrite {
  my $self = shift;
  my %commandline = ();
  if (scalar(keys %{$self->{config}->{default_no_section}}) > 0) {
    foreach (keys %{$self->{config}->{default_no_section}}) {
      $commandline{$_} = $self->{config}->{default_no_section}->{$_};
    }
  }
  if (exists $self->{config}->{$self->{section}}) {
    foreach (keys %{$self->{config}->{$self->{section}}}) {
      $commandline{$_} = $self->{config}->{$self->{section}}->{$_};
    }
  }
  foreach (keys %commandline) {
    if (! exists $self->{commandline}->{$_}) {
      $self->{commandline}->{$_} = $commandline{$_};
    }
  }
}



package main;

use strict;
use Getopt::Long qw(:config no_ignore_case);
use File::Basename;
use lib dirname($0);


use vars qw ($PROGNAME $REVISION $CONTACT $TIMEOUT $STATEFILESDIR $needs_restart %commandline);

$PROGNAME = "check_db2_health";
$REVISION = '$Revision: 1.0.3 $';
$CONTACT = 'gerhard.lausser@consol.de';
$TIMEOUT = 60;
$STATEFILESDIR = '/var/tmp/check_db2_health';
$needs_restart = 0;

my @modes = (
  ['server::connectiontime',
      'connection-time', undef,
      'Time to connect to the database' ],
  ['server::instance::database::connectedusers',
      'connected-users', undef,
      'Number of currently connected users' ],
  ['server::sql',
      'sql', undef,
      'any sql command returning a single number' ],
  ['server::instance::database::usage',
      'database-usage', undef,
      'Used space at the database level' ],
  ['server::instance::database::srp',
      'synchronous-read-percentage', undef,
      'Percentage of synchronous reads' ],
  ['server::instance::database::awp',
      'asynchronous-write-percentage', undef,
      'Percentage of asynchronous writes' ],
  ['server::instance::database::tablespace::usage',
      'tablespace-usage', undef,
      'Used space at the tablespace level' ],
  ['server::instance::database::tablespace::free',
      'tablespace-free', undef,
      'Free space at the tablespace level' ],
  ['server::instance::database::bufferpool::hitratio',
      'bufferpool-hitratio', undef,
      'Hit ratio of a buffer pool' ],
  ['server::instance::database::bufferpool::hitratiodata',
      'bufferpool-data-hitratio', undef,
      'Hit ratio of a buffer pool (data pages only)' ],
  ['server::instance::database::bufferpool::hitratioindex',
      'bufferpool-index-hitratio', undef,
      'Hit ratio of a buffer pool (indexes only)' ],
  ['server::instance::database::indexusage',
      'index-usage', undef,
      'Percentage of selects that use an index' ],
  ['server::instance::database::staletablerunstats',
      'stale-table-runstats', undef,
      'Tables whose statistics haven\'t been updated for a while' ],
  ['server::instance::database::lock::deadlocks',
      'deadlocks', undef,
      'Number of deadlocks per second' ],
  ['server::instance::database::lock::lockwaits',
      'lock-waits', undef,
      'Number of lock waits per second' ],
  ['server::instance::database::lock::lockwaiting',
      'lock-waiting', undef,
      'Percentage of the time locks spend waiting' ],
  ['server::instance::database::sortoverflows',
      'sort-overflows', undef,
      'Number of sort overflows per second (Sorts needing temporary tables on disk)' ],
  ['server::instance::database::sortoverflowpercentage',
      'sort-overflow-percentage', undef,
      'Percentage of sorts which result in an overflow' ],
  ['server::instance::database::logutilization',
      'log-utilization', undef,
      'Log utilization for a database' ],
  ['server::instance::database::lastbackup',
      'last-backup', undef,
      'Time (in days) since the database was last backupped' ],
  ['server::instance::listdatabases',
      'list-databases', undef,
      'convenience function which lists all databases' ],
  ['server::instance::database::listtablespaces',
      'list-tablespaces', undef,
      'convenience function which lists all tablespaces' ],
  ['server::instance::database::listbufferpools',
      'list-bufferpools', undef,
      'convenience function which lists all buffer pools' ],
  #  health SELECT * FROM TABLE(HEALTH_DB_HI('',-1)) AS T
);

sub print_usage () {
  print <<EOUS;
  Usage:
    $PROGNAME [-v] [-t <timeout>] --hostname <dbhost> --port <dbport,def: 50000>
        --username=<username> --password=<password> --mode=<mode>
        --database=<database>
    $PROGNAME [-h | --help]
    $PROGNAME [-V | --version]

  Options:
    --connect
       the connect string
    --username
       the db2 user
    --password
       the db2 user's password
    --database
       the database you want to check
    --warning
       the warning range
    --critical
       the critical range
    --mode
       the mode of the plugin. select one of the following keywords:
EOUS
  my $longest = length ((reverse sort {length $a <=> length $b} map { $_->[1] } @modes)[0]);
  my $format = "       %-".
  (length ((reverse sort {length $a <=> length $b} map { $_->[1] } @modes)[0])).
  "s\t(%s)\n";
  foreach (@modes) {
    printf $format, $_->[1], $_->[3];
  }
  printf "\n";
  print <<EOUS;
    --name
       the name of the tablespace, datafile, wait event, 
       latch, enqueue, or sql statement depending on the mode.
    --name2
       if name is a sql statement, this statement would appear in
       the output and the performance data. This can be ugly, so 
       name2 can be used to appear instead.
    --regexp
       if this parameter is used, name will be interpreted as a 
       regular expression.
    --units
       one of %, KB, MB, GB. This is used for a better output of mode=sql
       and for specifying thresholds for mode=tablespace-free

  Tablespace-related modes check all tablespaces in one run by default.
  If only a single tablespace should be checked, use the --name parameter.
  The same applies to databuffercache-related modes.

  In mode sql you can url-encode the statement so you will not have to mess
  around with special characters in your Nagios service definitions.
  Instead of 
  --name="select count(*) from v\$session where status = 'ACTIVE'"
  you can say 
  --name=select%20count%28%2A%29%20from%20v%24session%20where%20status%20%3D%20%27ACTIVE%27
  For your convenience you can call check_db2_health with the --encode
  option and it will encode the standard input.

EOUS
  
}

sub print_help () {
  print "Copyright (c) Gerhard Lausser\n\n";
  print "\n";
  print "  Check various parameters of DB2 databases \n";
  print "\n";
  print_usage();
  support();
}


sub print_revision ($$) {
  my $commandName = shift;
  my $pluginRevision = shift;
  $pluginRevision =~ s/^\$Revision: //;
  $pluginRevision =~ s/ \$\s*$//;
  print "$commandName ($pluginRevision)\n";
  print "This nagios plugin comes with ABSOLUTELY NO WARRANTY. You may redistribute\ncopies of this plugin under the terms of the GNU General Public License.\n";
}

sub support () {
  my $support='Send email to gerhard.lausser@consol.de if you have questions\nregarding use of this software. \nPlease include version information with all correspondence (when possible,\nuse output from the --version option of the plugin itself).\n';
  $support =~ s/@/\@/g;
  $support =~ s/\\n/\n/g;
  print $support;
}

sub contact_author ($$) {
  my $item = shift;
  my $strangepattern = shift;
  if ($commandline{verbose}) {
    printf STDERR
        "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n".
        "You found a line which is not recognized by %s\n".
        "This means, certain components of your system cannot be checked.\n".
        "Please contact the author %s and\nsend him the following output:\n\n".
        "%s /%s/\n\nThank you!\n".
        "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n",
            $PROGNAME, $CONTACT, $item, $strangepattern;
  }
}

%commandline = ();
my @params = (
    "timeout|t=i",
    "version|V",
    "help|h",
    "verbose|v",
    "debug|d",
    "hostname=s",
    "username=s",
    "password=s",
    "port=i",
    "mode|m=s",
    "database=s",
    "tablespace=s",
    "datafile=s",
    "waitevent=s",
    "name=s",
    "name2=s",
    "regexp",
    "perfdata",
    "warning=s",
    "critical=s",
    "dbthresholds:s",
    "absolute|a",
    "basis",
    "lookback|l=i",
    "environment|e=s%",
    "method=s",
    "runas|r=s",
    "scream",
    "shell",
    "eyecandy",
    "encode",
    "units=s",
    "3",
    "with-mymodules-dyn-dir=s",
    "report=s",
    "extra-opts:s");

if (! GetOptions(\%commandline, @params)) {
  print_help();
  exit $ERRORS{UNKNOWN};
}

if (exists $commandline{'extra-opts'}) {
  # read the extra file and overwrite other parameters
  my $extras = Extraopts->new(file => $commandline{'extra-opts'}, commandline => \%commandline);
  if (! $extras->is_valid()) {
    printf "extra-opts are not valid: %s\n", $extras->{errors};
    exit $ERRORS{UNKNOWN};
  } else {
    $extras->overwrite();
  }
}

if (exists $commandline{version}) {
  print_revision($PROGNAME, $REVISION);
  exit $ERRORS{OK};
}

if (exists $commandline{help}) {
  print_help();
  exit $ERRORS{OK};
} elsif (! exists $commandline{mode}) {
  printf "Please select a mode\n";
  print_help();
  exit $ERRORS{OK};
}

if ($commandline{mode} eq "encode") {
  my $input = <>;
  chomp $input;
  $input =~ s/([^A-Za-z0-9])/sprintf("%%%02X", ord($1))/seg;
  printf "%s\n", $input;
  exit $ERRORS{OK};
}

if (exists $commandline{3}) {
  $ENV{NRPE_MULTILINESUPPORT} = 1;
}

if (exists $commandline{timeout}) {
  $TIMEOUT = $commandline{timeout};
}

if (exists $commandline{verbose}) {
  $DBD::DB2::Server::verbose = exists $commandline{verbose};
}

if (exists $commandline{scream}) {
#  $DBD::DB2::Server::hysterical = exists $commandline{scream};
}

if (exists $commandline{method}) {
  # dbi, snmp or sqlplus
} else {
  $commandline{method} = "dbi";
}

if (exists $commandline{report}) {
  # short, long, html
} else {
  $commandline{report} = "long";
}

if (exists $commandline{'with-mymodules-dyn-dir'}) {
  $DBD::DB2::Server::my_modules_dyn_dir = $commandline{'with-mymodules-dyn-dir'};
} else {
  $DBD::DB2::Server::my_modules_dyn_dir = '/usr/local/nagios/libexec';
}

if (exists $commandline{environment}) {
  # if the desired environment variable values are different from
  # the environment of this running script, then a restart is necessary.
  # because setting $ENV does _not_ change the environment of the running script.
  foreach (keys %{$commandline{environment}}) {
    if ((! $ENV{$_}) || ($ENV{$_} ne $commandline{environment}->{$_})) {
      $needs_restart = 1;
      $ENV{$_} = $commandline{environment}->{$_};
      printf STDERR "new %s=%s forces restart\n", $_, $ENV{$_} 
          if $DBD::DB2::Server::verbose;
    }
  }
  # e.g. called with --runas dbnagio. shlib_path environment variable is stripped
  # during the sudo.
  # so the perl interpreter starts without a shlib_path. but --runas cares for
  # a --environment shlib_path=...
  # so setting the environment variable in the code above and restarting the 
  # perl interpreter will help it find shared libs
}

if (exists $commandline{runas}) {
  # remove the runas parameter
  # exec sudo $0 ... the remaining parameters
  $needs_restart = 1;
  # if the calling script has a path for shared libs and there is no --environment
  # parameter then the called script surely needs the variable too.
  foreach my $important_env qw(LD_LIBRARY_PATH SHLIB_PATH 
      DB2_HOME) {
    if ($ENV{$important_env} && ! scalar(grep { /^$important_env=/ } 
        keys %{$commandline{environment}})) {
      $commandline{environment}->{$important_env} = $ENV{$important_env};
      printf STDERR "add important --environment %s=%s\n", 
          $important_env, $ENV{$important_env} if $DBD::DB2::Server::verbose;
    }
  }
}

if ($needs_restart) {
  my @newargv = ();
  my $runas = undef;
  if (exists $commandline{runas}) {
    $runas = $commandline{runas};
    delete $commandline{runas};
  }
  foreach my $option (keys %commandline) {
    if (grep { /^$option/ && /=/ } @params) {
      if (ref ($commandline{$option}) eq "HASH") {
        foreach (keys %{$commandline{$option}}) {
          push(@newargv, sprintf "--%s", $option);
          push(@newargv, sprintf "%s=%s", $_, $commandline{$option}->{$_});
        }
      } else {
        push(@newargv, sprintf "--%s", $option);
        push(@newargv, sprintf "%s", $commandline{$option});
      }
    } else {
      push(@newargv, sprintf "--%s", $option);
    }
  }
  if ($runas && ($> == 0)) {
    # this was not my idea. some people connect as root to their nagios clients.
    exec "su", "-c", sprintf("%s %s", $0, join(" ", @newargv)), "-", $runas;
  } elsif ($runas) {
    exec "sudo", "-S", "-u", $runas, $0, @newargv;
  } else {
    exec $0, @newargv;  
    # this makes sure that even a SHLIB or LD_LIBRARY_PATH are set correctly
    # when the perl interpreter starts. Setting them during runtime does not
    # help loading e.g. libclntsh.so
  }
  exit;
}

if (exists $commandline{shell}) {
  # forget what you see here.
  system("/bin/sh");
}

if (exists $commandline{name}) {
  # objects can be encoded like an url
  # with s/([^A-Za-z0-9])/sprintf("%%%02X", ord($1))/seg;
  if (($commandline{mode} ne "sql") || 
      (($commandline{mode} eq "sql") &&
       ($commandline{name} =~ /select%20/i))) { # protect ... like '%cac%' ... from decoding
    $commandline{name} =~ s/\%([A-Fa-f0-9]{2})/pack('C', hex($1))/seg;
  }
  if ($commandline{name} =~ /^0$/) {
    # without this, $params{selectname} would be treated like undef
    $commandline{name} = "00";
  } 
}

$SIG{'ALRM'} = sub {
  printf "UNKNOWN - %s timed out after %d seconds\n", $PROGNAME, $TIMEOUT;
  exit $ERRORS{UNKNOWN};
};
alarm($TIMEOUT);

my $nagios_level = $ERRORS{UNKNOWN};
my $nagios_message = "";
my $perfdata = "";
my $racmode = 0;
if ($commandline{mode} =~ /^rac-([^\-.]+)/) {
  $racmode = 1;
  $commandline{mode} =~ s/^rac\-//g;
}
if ($commandline{mode} =~ /^my-([^\-.]+)/) {
  my $param = $commandline{mode};
  $param =~ s/\-/::/g;
  push(@modes, [$param, $commandline{mode}, undef, 'my extension']);
} elsif ((! grep { $commandline{mode} eq $_ } map { $_->[1] } @modes) &&
    (! grep { $commandline{mode} eq $_ } map { defined $_->[2] ? @{$_->[2]} : () } @modes)) {
  printf "UNKNOWN - mode %s\n", $commandline{mode};
  print_usage();
  exit 3;
}

my %params = (
    timeout => $TIMEOUT,
    mode => (
        map { $_->[0] } 
        grep {
           ($commandline{mode} eq $_->[1]) || 
           ( defined $_->[2] && grep { $commandline{mode} eq $_ } @{$_->[2]}) 
        } @modes
    )[0],
    cmdlinemode => $commandline{mode},
    method => ($commandline{method} ||
        $ENV{NAGIOS__SERVICEDB2_METH} ||
        $ENV{NAGIOS__HOSTDB2_METH} || 'dbi'),
    hostname => ($commandline{hostname}  || 
        $ENV{NAGIOS__SERVICEDB2_HOST} ||
        $ENV{NAGIOS__HOSTDB2_HOST}),
    username => ($commandline{username} || 
        $ENV{NAGIOS__SERVICEDB2_USER} ||
        $ENV{NAGIOS__HOSTDB2_USER}),
    password => ($commandline{password} || 
        $ENV{NAGIOS__SERVICEDB2_PASS} ||
        $ENV{NAGIOS__HOSTDB2_PASS}),
    port => ($commandline{port} || 
        $ENV{NAGIOS__SERVICEDB2_PORT} ||
        $ENV{NAGIOS__HOSTDB2_PORT}),
    database => ($commandline{database} || 
        $ENV{NAGIOS__SERVICEDB2_DATABASE} ||
        $ENV{NAGIOS__HOSTDB2_DATABASE}),
    warningrange => $commandline{warning},
    criticalrange => $commandline{critical},
    dbthresholds => $commandline{dbthresholds},
    absolute => $commandline{absolute},
    lookback => $commandline{lookback},
    tablespace => $commandline{tablespace},
    datafile => $commandline{datafile},
    basis => $commandline{basis},
    selectname => $commandline{name} || $commandline{tablespace} || $commandline{datafile},
    regexp => $commandline{regexp},
    name => $commandline{name},
    name2 => $commandline{name2} || $commandline{name},
    units => $commandline{units},
    eyecandy => $commandline{eyecandy},
    statefilesdir => $STATEFILESDIR,
    verbose => $commandline{verbose},
    report => $commandline{report},
);
my $server = undef;

$server = DBD::DB2::Server->new(%params);
$server->nagios(%params);
$server->calculate_result();
$nagios_message = $server->{nagios_message};
$nagios_level = $server->{nagios_level};
$perfdata = $server->{perfdata};

printf "%s - %s", $ERRORCODES{$nagios_level}, $nagios_message;
printf " | %s", $perfdata if $perfdata;
printf "\n";
exit $nagios_level;
