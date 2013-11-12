#! /usr/bin/perl -w

my %ERRORS=( OK => 0, WARNING => 1, CRITICAL => 2, UNKNOWN => 3 );
my %ERRORCODES=( 0 => 'OK', 1 => 'WARNING', 2 => 'CRITICAL', 3 => 'UNKNOWN' );
package DBD::MSSQL::Server::Memorypool;

use strict;

our @ISA = qw(DBD::MSSQL::Server);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    buffercache => undef,
    procedurecache => undef,
    locks => [],
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::memorypool::buffercache/) {
    $self->{buffercache} = DBD::MSSQL::Server::Memorypool::BufferCache->new(
        %params);
  } elsif ($params{mode} =~ /server::memorypool::procedurecache/) {
    $self->{procedurecache} = DBD::MSSQL::Server::Memorypool::ProcedureCache->new(
        %params);
  } elsif ($params{mode} =~ /server::memorypool::lock/) {
    DBD::MSSQL::Server::Memorypool::Lock::init_locks(%params);
    if (my @locks = DBD::MSSQL::Server::Memorypool::Lock::return_locks()) {
      $self->{locks} = \@locks;
    } else {
      $self->add_nagios_critical("unable to aquire lock info");
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if ($params{mode} =~ /server::memorypool::buffercache/) {
    $self->{buffercache}->nagios(%params);
    $self->merge_nagios($self->{buffercache});
  } elsif ($params{mode} =~ /^server::memorypool::lock::listlocks/) {
    foreach (sort { $a->{name} cmp $b->{name}; }  @{$self->{locks}}) {
      printf "%s\n", $_->{name};
    }
    $self->add_nagios_ok("have fun");
  } elsif ($params{mode} =~ /^server::memorypool::lock/) {
    foreach (@{$self->{locks}}) {
      $_->nagios(%params);
      $self->merge_nagios($_);
    }
  }
}


package DBD::MSSQL::Server::Memorypool::BufferCache;

use strict;

our @ISA = qw(DBD::MSSQL::Server::Memorypool);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    hitratio => undef,
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
  if ($params{mode} =~ /server::memorypool::buffercache::hitratio/) {
    #        -- (a.cntr_value * 1.0 / b.cntr_value) * 100.0 [BufferCacheHitRatio]
    $self->{cnt_hitratio} = $self->{handle}->get_perf_counter(
        "SQLServer:Buffer Manager", "Buffer cache hit ratio");
    $self->{cnt_hitratio_base} = $self->{handle}->get_perf_counter(
        "SQLServer:Buffer Manager", "Buffer cache hit ratio base");
    if (! defined $self->{cnt_hitratio}) {
      $self->add_nagios_unknown("unable to aquire buffer cache data");
    } else {
      # das kracht weil teilweise negativ
      #$self->valdiff(\%params, qw(cnt_hitratio cnt_hitratio_base));
      $self->{hitratio} = ($self->{cnt_hitratio_base} == 0) ?
          100 : $self->{cnt_hitratio} / $self->{cnt_hitratio_base} * 100.0;
      # soll vorkommen.....
      $self->{hitratio} = 100 if ($self->{hitratio} > 100);
    }
  } elsif ($params{mode} =~ /server::memorypool::buffercache::lazywrites/) {
    $self->{lazy_writes_s} = $self->{handle}->get_perf_counter(
        "SQLServer:Buffer Manager", "Lazy writes/sec");
    if (! defined $self->{lazy_writes_s}) {
      $self->add_nagios_unknown("unable to aquire buffer manager data");
    } else {
      $self->valdiff(\%params, qw(lazy_writes_s));
      $self->{lazy_writes_per_sec} = $self->{delta_lazy_writes_s} / $self->{delta_timestamp};
    }
  } elsif ($params{mode} =~ /server::memorypool::buffercache::pagelifeexpectancy/) {
    $self->{pagelifeexpectancy} = $self->{handle}->get_perf_counter(
        "SQLServer:Buffer Manager", "Page life expectancy");
    if (! defined $self->{pagelifeexpectancy}) {
      $self->add_nagios_unknown("unable to aquire buffer manager data");
    }
  } elsif ($params{mode} =~ /server::memorypool::buffercache::freeliststalls/) {
    $self->{freeliststalls_s} = $self->{handle}->get_perf_counter(
        "SQLServer:Buffer Manager", "Free list stalls/sec");
    if (! defined $self->{freeliststalls_s}) {
      $self->add_nagios_unknown("unable to aquire buffer manager data");
    } else {
      $self->valdiff(\%params, qw(freeliststalls_s));
      $self->{freeliststalls_per_sec} = $self->{delta_freeliststalls_s} / $self->{delta_timestamp};
    }
  } elsif ($params{mode} =~ /server::memorypool::buffercache::checkpointpages/) {
    $self->{checkpointpages_s} = $self->{handle}->get_perf_counter(
        "SQLServer:Buffer Manager", "Checkpoint pages/sec");
    if (! defined $self->{checkpointpages_s}) {
      $self->add_nagios_unknown("unable to aquire buffer manager data");
    } else {
      $self->valdiff(\%params, qw(checkpointpages_s));
      $self->{checkpointpages_per_sec} = $self->{delta_checkpointpages_s} / $self->{delta_timestamp};
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::memorypool::buffercache::hitratio/) {
      $self->add_nagios(
          $self->check_thresholds($self->{hitratio}, '90:', '80:'),
          sprintf "buffer cache hit ratio is %.2f%%", $self->{hitratio});
      $self->add_perfdata(sprintf "buffer_cache_hit_ratio=%.2f%%;%s;%s",
          $self->{hitratio},
          $self->{warningrange}, $self->{criticalrange});
      #$self->add_perfdata(sprintf "buffer_cache_hit_ratio_now=%.2f%%",
      #    $self->{hitratio_now});
    } elsif ($params{mode} =~ /server::memorypool::buffercache::lazywrites/) {
      $self->add_nagios(
          $self->check_thresholds($self->{lazy_writes_per_sec}, '20', '40'),
          sprintf "%.2f lazy writes per second", $self->{lazy_writes_per_sec});
      $self->add_perfdata(sprintf "lazy_writes_per_sec=%.2f;%s;%s",
          $self->{lazy_writes_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::memorypool::buffercache::pagelifeexpectancy/) {
      $self->add_nagios(
          $self->check_thresholds($self->{pagelifeexpectancy}, '300:', '180:'),
          sprintf "page life expectancy is %d seconds", $self->{pagelifeexpectancy});
      $self->add_perfdata(sprintf "page_life_expectancy=%d;%s;%s",
          $self->{pagelifeexpectancy},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::memorypool::buffercache::freeliststalls/) {
      $self->add_nagios(
          $self->check_thresholds($self->{freeliststalls_per_sec}, '4', '10'),
          sprintf "%.2f free list stalls per second", $self->{freeliststalls_per_sec});
      $self->add_perfdata(sprintf "free_list_stalls_per_sec=%.2f;%s;%s",
          $self->{freeliststalls_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::memorypool::buffercache::checkpointpages/) {
      $self->add_nagios(
          $self->check_thresholds($self->{checkpointpages_per_sec}, '100', '500'),
          sprintf "%.2f pages flushed per second", $self->{checkpointpages_per_sec});
      $self->add_perfdata(sprintf "checkpoint_pages_per_sec=%.2f;%s;%s",
          $self->{checkpointpages_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}





package DBD::MSSQL::Server::Memorypool::Lock;

use strict;

our @ISA = qw(DBD::MSSQL::Server::Memorypool);


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
    if (($params{mode} =~ /server::memorypool::lock::listlocks/) ||
        ($params{mode} =~ /server::memorypool::lock::waits/) ||
        ($params{mode} =~ /server::memorypool::lock::deadlocks/) ||
        ($params{mode} =~ /server::memorypool::lock::timeouts/)) {
      my @lockresult = $params{handle}->get_instance_names(
          'SQLServer:Locks');
      foreach (@lockresult) {
        my ($name) = @{$_};
        $name =~ s/\s*$//;
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        my $lock = DBD::MSSQL::Server::Memorypool::Lock->new(
            %thisparams);
        add_lock($lock);
        $num_locks++;
      }
      if (! $num_locks) {
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
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    name => $params{name},
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $self->init_nagios();
  if ($params{mode} =~ /server::memorypool::lock::listlocks/) {
    # name reicht
  } elsif ($params{mode} =~ /server::memorypool::lock::waits/) {
    $self->{lock_waits_s} = $self->{handle}->get_perf_counter_instance(
        "SQLServer:Locks", "Lock Waits/sec", $self->{name});
    if (! defined $self->{lock_waits_s}) {
      $self->add_nagios_unknown("unable to aquire counter data");
    } else {
      $self->valdiff(\%params, qw(lock_waits_s));
      $self->{lock_waits_per_sec} = $self->{delta_lock_waits_s} / $self->{delta_timestamp};
    }
  } elsif ($params{mode} =~ /^server::memorypool::lock::timeouts/) {
    $self->{lock_timeouts_s} = $self->{handle}->get_perf_counter_instance(
        "SQLServer:Locks", "Lock Timeouts/sec", $self->{name});
    if (! defined $self->{lock_timeouts_s}) {
      $self->add_nagios_unknown("unable to aquire counter data");
    } else {
      $self->valdiff(\%params, qw(lock_timeouts_s));
      $self->{lock_timeouts_per_sec} = $self->{delta_lock_timeouts_s} / $self->{delta_timestamp};
    }
  } elsif ($params{mode} =~ /^server::memorypool::lock::deadlocks/) {
    $self->{lock_deadlocks_s} = $self->{handle}->get_perf_counter_instance(
        "SQLServer:Locks", "Number of Deadlocks/sec", $self->{name});
    if (! defined $self->{lock_deadlocks_s}) {
      $self->add_nagios_unknown("unable to aquire counter data");
    } else {
      $self->valdiff(\%params, qw(lock_deadlocks_s));
      $self->{lock_deadlocks_per_sec} = $self->{delta_lock_deadlocks_s} / $self->{delta_timestamp};
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::memorypool::lock::waits/) {
      $self->add_nagios(
          $self->check_thresholds($self->{lock_waits_per_sec}, 100, 500),
          sprintf "%.4f lock waits / sec for %s",
          $self->{lock_waits_per_sec}, $self->{name});
      $self->add_perfdata(sprintf "%s_lock_waits_per_sec=%.4f;%s;%s",
          $self->{name}, $self->{lock_waits_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::memorypool::lock::timeouts/) {
      $self->add_nagios(
          $self->check_thresholds($self->{lock_timeouts_per_sec}, 1, 5),
          sprintf "%.4f lock timeouts / sec for %s",
          $self->{lock_timeouts_per_sec}, $self->{name});
      $self->add_perfdata(sprintf "%s_lock_timeouts_per_sec=%.4f;%s;%s",
          $self->{name}, $self->{lock_timeouts_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::memorypool::lock::deadlocks/) {
      $self->add_nagios(
          $self->check_thresholds($self->{lock_deadlocks_per_sec}, 1, 5),
          sprintf "%.4f deadlocks / sec for %s",
          $self->{lock_deadlocks_per_sec}, $self->{name});
      $self->add_perfdata(sprintf "%s_deadlocks_per_sec=%.4f;%s;%s",
          $self->{name}, $self->{lock_deadlocks_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}


package DBD::MSSQL::Server::Memorypool::SystemLevelDataStructures::LockTable;
package DBD::MSSQL::Server::Memorypool::ProcedureCache;
package DBD::MSSQL::Server::Memorypool::LogCache;
package DBD::MSSQL::Server::Memorypool::SystemLevelDataStructures;
package DBD::MSSQL::Server::Database::Datafile;

use strict;
use File::Basename;

our @ISA = qw(DBD::MSSQL::Server::Database);


{
  my @datafiles = ();
  my $initerrors = undef;

  sub add_datafile {
    push(@datafiles, shift);
  }

  sub return_datafiles {
    return reverse
        sort { $a->{logicalfilename} cmp $b->{logicalfilename} } @datafiles;
  }

  sub clear_datafiles {
    @datafiles = ();
  }

  sub init_datafiles {
    my %params = @_;
    my $num_datafiles = 0;
    if ($params{mode} =~ /server::database::datafile::listdatafiles/) {
      my @datafileresults = $params{handle}->fetchall_array(q{
DECLARE @DBInfo TABLE
( ServerName VARCHAR(100),
DatabaseName VARCHAR(100),
FileSizeMB INT,
LogicalFileName sysname,
PhysicalFileName NVARCHAR(520),
Status sysname,
Updateability sysname,
RecoveryMode sysname,
FreeSpaceMB INT,
FreeSpacePct VARCHAR(7),
FreeSpacePages INT,
PollDate datetime)

DECLARE @command VARCHAR(5000)

SELECT @command = 'Use [' + '?' + '] SELECT
@@servername as ServerName,
' + '''' + '?' + '''' + ' AS DatabaseName,
CAST(sysfiles.size/128.0 AS int) AS FileSize,
sysfiles.name AS LogicalFileName, sysfiles.filename AS PhysicalFileName,
CONVERT(sysname,DatabasePropertyEx(''?'',''Status'')) AS Status,
CONVERT(sysname,DatabasePropertyEx(''?'',''Updateability'')) AS Updateability,
CONVERT(sysname,DatabasePropertyEx(''?'',''Recovery'')) AS RecoveryMode,
CAST(sysfiles.size/128.0 - CAST(FILEPROPERTY(sysfiles.name, ' + '''' +
       'SpaceUsed' + '''' + ' ) AS int)/128.0 AS int) AS FreeSpaceMB,
CAST(100 * (CAST (((sysfiles.size/128.0 -CAST(FILEPROPERTY(sysfiles.name,
' + '''' + 'SpaceUsed' + '''' + ' ) AS int)/128.0)/(sysfiles.size/128.0))
AS decimal(4,2))) AS varchar(8)) + ' + '''' + '%' + '''' + ' AS FreeSpacePct,
GETDATE() as PollDate FROM dbo.sysfiles'
INSERT INTO @DBInfo
   (ServerName,
   DatabaseName,
   FileSizeMB,
   LogicalFileName,
   PhysicalFileName,
   Status,
   Updateability,
   RecoveryMode,
   FreeSpaceMB,
   FreeSpacePct,
   PollDate)
EXEC sp_MSForEachDB @command

SELECT
   ServerName,
   DatabaseName,
   FileSizeMB,
   LogicalFileName,
   PhysicalFileName,
   Status,
   Updateability,
   RecoveryMode,
   FreeSpaceMB,
   FreeSpacePct,
   PollDate
FROM @DBInfo
ORDER BY
   ServerName,
   DatabaseName
      });
      if (DBD::MSSQL::Server::return_first_server()->windows_server()) {
        fileparse_set_fstype("MSWin32");
      }
      foreach (@datafileresults) {
        my ($servername, $databasename, $filesizemb, $logicalfilename,
            $physicalfilename, $status, $updateability, $recoverymode,
            $freespacemb, $freespacepct, $polldate) = @{$_};
        next if $databasename ne $params{database};
        if ($params{regexp}) {
          #next if $params{selectname} &&
          #    (($name !~ /$params{selectname}/) &&
          #    (basename($name) !~ /$params{selectname}/));
          next if $params{selectname} &&
              ($logicalfilename !~ /$params{selectname}/);
        } else {
          #next if $params{selectname} &&
          #    ((lc $params{selectname} ne lc $name) &&
          #    (lc $params{selectname} ne lc basename($name)));
          next if $params{selectname} &&
              (lc $params{selectname} ne lc $logicalfilename);
        }
        my %thisparams = %params;
        $thisparams{servername} = $servername;
        $thisparams{databasename} = $databasename;
        $thisparams{filesizemb} = $filesizemb;
        $thisparams{logicalfilename} = $logicalfilename;
        $thisparams{servername} = $servername;
        $thisparams{status} = $status;
        $thisparams{updateability} = $updateability;
        $thisparams{recoverymode} = $recoverymode;
        $thisparams{freespacemb} = $freespacemb;
        $thisparams{freespacepct} = $freespacepct;
        $thisparams{polldate} = $polldate;
        my $datafile = 
            DBD::MSSQL::Server::Database::Datafile->new(
            %thisparams);
        add_datafile($datafile);
        $num_datafiles++;
      }
    }
  }

}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    databasename => $params{databasename},
    filesizemb => $params{filesizemb},
    logicalfilename => $params{logicalfilename},
    physicalfilename => $params{physicalfilename},
    status => $params{status},
    updateability => $params{updateability},
    recoverymode => $params{recoverymode},
    freespacemb => $params{freespacemb},
    freespacepct => $params{freespacepct},
    freespacepages => $params{freespacepages},
    polldate => $params{polldate},
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
  if ($params{mode} =~ /server::database::iobalance/) {
    if (! defined $self->{phyrds}) {
      $self->add_nagios_critical(sprintf "unable to read datafile io %s", $@);
    } else {
      $params{differenciator} = $self->{path};
      $self->valdiff(\%params, qw(phyrds phywrts));
      $self->{io_total} = $self->{delta_phyrds} + $self->{delta_phywrts};
    }
  } elsif ($params{mode} =~ /server::database::datafile::iotraffic/) {
    if (! defined $self->{phyrds}) {
      $self->add_nagios_critical(sprintf "unable to read datafile io %s", $@);
    } else {
      $params{differenciator} = $self->{path};
      $self->valdiff(\%params, qw(phyrds phywrts));
      $self->{io_total_per_sec} = ($self->{delta_phyrds} + $self->{delta_phywrts}) /
          $self->{delta_timestamp};
    }
  }
}


sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::database::datafile::iotraffic/) {
      $self->add_nagios(
          $self->check_thresholds($self->{io_total_per_sec}, "1000", "5000"),
          sprintf ("%s: %.2f IO Operations per Second", 
              $self->{name}, $self->{io_total_per_sec}));
      $self->add_perfdata(sprintf "'dbf_%s_io_total_per_sec'=%.2f;%d;%d",
          $self->{name}, $self->{io_total_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    }
  }
}

package DBD::MSSQL::Server::Database;

use strict;

our @ISA = qw(DBD::MSSQL::Server);


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
    my $cache_db_sizes = {};
    if (($params{mode} =~ /server::database::listdatabases/) ||
        ($params{mode} =~ /server::database::databasefree/) ||
        ($params{mode} =~ /server::database::lastbackup/) ||
        ($params{mode} =~ /server::database::transactions/) ||
        ($params{mode} =~ /server::database::datafile/)) {
      my @databaseresult = ();
      if ($params{product} eq "MSSQL") {
        if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
          @databaseresult = $params{handle}->fetchall_array(q{
            SELECT name, database_id, state FROM master.sys.databases
          });
        } else {
          @databaseresult = $params{handle}->fetchall_array(q{
            SELECT name, dbid, status FROM master.dbo.sysdatabases
          });
        }
      } elsif ($params{product} eq "ASE") {
        # erst ab 15.x
        my @sysusageresult = $params{handle}->fetchall_array(q{
            SELECT
              db_name(d.dbid) AS db_name,
              SUM(
                CASE WHEN u.segmap != 4 
                     THEN u.size/1048576.*@@maxpagesize 
                END)
              AS data_size,
              SUM(
                CASE WHEN u.segmap != 4 
                     THEN size - curunreservedpgs(u.dbid, u.lstart, u.unreservedpgs)
                END)/1048576.*@@maxpagesize
              AS data_used,
              SUM(
                CASE WHEN u.segmap = 4 
                     THEN u.size/1048576.*@@maxpagesize
                END)
              AS log_size,
              SUM(
                CASE WHEN u.segmap = 4
                     THEN u.size/1048576.*@@maxpagesize
                END) - lct_admin("logsegment_freepages",d.dbid)/1048576.*@@maxpagesize
              AS log_used
              FROM master..sysdatabases d, master..sysusages u
              WHERE u.dbid = d.dbid AND d.status != 256
              GROUP BY d.dbid
              ORDER BY db_name(d.dbid)
        });
        foreach (@sysusageresult) {
          my($db_name, $data_size, $data_used, $log_size, $log_used) = @{$_};
          $log_size = 0 if ! defined $log_size;
          $log_used = 0 if ! defined $log_used;
          $cache_db_sizes->{$db_name} = {
              'db_name' => $db_name, 
              'data_size' => $data_size,
              'data_used' => $data_used,
              'log_size' => $log_size,
              'log_used' => $log_used,
          };
          $cache_db_sizes->{$db_name}->{data_used_pct} = 100 *
              $cache_db_sizes->{$db_name}->{data_used} /
              $cache_db_sizes->{$db_name}->{data_size};
          $cache_db_sizes->{$db_name}->{log_used_pct} = 
              $cache_db_sizes->{$db_name}->{log_size} ?
              100 *
              $cache_db_sizes->{$db_name}->{log_used} /
              $cache_db_sizes->{$db_name}->{log_size}
              : 0;
          $cache_db_sizes->{$db_name}->{data_free} =
              $cache_db_sizes->{$db_name}->{data_size} - 
              $cache_db_sizes->{$db_name}->{data_used};
          $cache_db_sizes->{$db_name}->{log_free} =
              $cache_db_sizes->{$db_name}->{log_size} - 
              $cache_db_sizes->{$db_name}->{log_used};
        }
        @databaseresult = $params{handle}->fetchall_array(q{
          SELECT name, dbid, status2 FROM master.dbo.sysdatabases
        });
      }
      if ($params{mode} =~ /server::database::transactions/) {
        push(@databaseresult, [ '_Total', 0 ]);
      }
      foreach (sort {$a->[0] cmp $b->[0]} @databaseresult) {
        my ($name, $id, $state) = @{$_};
        next if $params{notemp} && $name eq "tempdb";
        next if $params{database} && $name ne $params{database};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{id} = $id;
        $thisparams{state} = $state;
        $thisparams{cache_db_sizes} = $cache_db_sizes;
        my $database = DBD::MSSQL::Server::Database->new(
            %thisparams);
        add_database($database);
        $num_databases++;
      }
      if (! $num_databases) {
        $initerrors = 1;
        return undef;
      }
    } elsif ($params{mode} =~ /server::database::online/) {
      my @databaseresult = ();
      if ($params{product} eq "MSSQL") {
        if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
          @databaseresult = $params{handle}->fetchall_array(q{
            SELECT name, state, state_desc, collation_name FROM master.sys.databases
          });
        }
      }
      foreach (@databaseresult) {
        my ($name, $state, $state_desc, $collation_name) = @{$_};
        next if $params{database} && $name ne $params{database};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{state} = $state;
        $thisparams{state_desc} = $state_desc;
        $thisparams{collation_name} = $collation_name;
        my $database = DBD::MSSQL::Server::Database->new(
            %thisparams);
        add_database($database);
        $num_databases++;
      }
      if (! $num_databases) {
        $initerrors = 1;
        return undef;
      }
    } elsif ($params{mode} =~ /server::database::auto(growths|shrinks)/) {
      my @databasenames = ();
      my @databaseresult = ();
      my $lookback = $params{lookback} || 30;
      if ($params{product} eq "MSSQL") {
        if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
          @databasenames = $params{handle}->fetchall_array(q{
            SELECT name FROM master.sys.databases
          });
          @databasenames = map { $_->[0] } @databasenames;
            # starttime = Oct 22 2012 01:51:41:373AM = DBD::Sybase datetype LONG
          my $sql = q{
              DECLARE @path NVARCHAR(1000)
              SELECT
                  @path = Substring(PATH, 1, Len(PATH) - Charindex('\', Reverse(PATH))) + '\log.trc'
              FROM
                  sys.traces
              WHERE
                  id = 1
              SELECT
                  databasename, COUNT(*)
              FROM 
                  ::fn_trace_gettable(@path, 0)
              INNER JOIN
                  sys.trace_events e
              ON
                  eventclass = trace_event_id
              INNER JOIN
                  sys.trace_categories AS cat
              ON
                  e.category_id = cat.category_id
              WHERE
                  e.name IN( EVENTNAME ) AND datediff(Minute, starttime, current_timestamp) < ?
              GROUP BY
                  databasename
          };
          if ($params{mode} =~ /server::database::autogrowths::file/) {
            $sql =~ s/EVENTNAME/'Data File Auto Grow', 'Log File Auto Grow'/;
          } elsif ($params{mode} =~ /server::database::autogrowths::logfile/) {
            $sql =~ s/EVENTNAME/'Log File Auto Grow'/;
          } elsif ($params{mode} =~ /server::database::autogrowths::datafile/) {
            $sql =~ s/EVENTNAME/'Data File Auto Grow'/;
          }
          if ($params{mode} =~ /server::database::autoshrinks::file/) {
            $sql =~ s/EVENTNAME/'Data File Auto Shrink', 'Log File Auto Shrink'/;
          } elsif ($params{mode} =~ /server::database::autoshrinks::logfile/) {
            $sql =~ s/EVENTNAME/'Log File Auto Shrink'/;
          } elsif ($params{mode} =~ /server::database::autoshrinks::datafile/) {
            $sql =~ s/EVENTNAME/'Data File Auto Shrink'/;
          }
          @databaseresult = $params{handle}->fetchall_array($sql, $lookback);
        }
      }
      foreach my $name (@databasenames) {
        next if $params{database} && $name ne $params{database};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my $autogrowshrink = eval {
            map { $_->[1] } grep { $_->[0] eq $name } @databaseresult;
        } || 0;
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{growshrinkinterval} = $lookback;
        $thisparams{autogrowshrink} = $autogrowshrink;
        my $database = DBD::MSSQL::Server::Database->new(
            %thisparams);
        add_database($database);
        $num_databases++;
      }
      if (! $num_databases) {
        $initerrors = 1;
        return undef;
      }
    } elsif ($params{mode} =~ /server::database::dbccshrinks/) {
      my @databasenames = ();
      my @databaseresult = ();
      my $lookback = $params{lookback} || 30;
      if ($params{product} eq "MSSQL") {
        if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
          @databasenames = $params{handle}->fetchall_array(q{
            SELECT name FROM master.sys.databases
          });
          @databasenames = map { $_->[0] } @databasenames;
            # starttime = Oct 22 2012 01:51:41:373AM = DBD::Sybase datetype LONG
          my $sql = q{
              DECLARE @path NVARCHAR(1000)
              SELECT
                  @path = Substring(PATH, 1, Len(PATH) - Charindex('\', Reverse(PATH))) + '\log.trc'
              FROM
                  sys.traces
              WHERE
                  id = 1
              SELECT
                  databasename, COUNT(*)
              FROM 
                  ::fn_trace_gettable(@path, 0)
              INNER JOIN
                  sys.trace_events e
              ON
                  eventclass = trace_event_id
              INNER JOIN
                  sys.trace_categories AS cat
              ON
                  e.category_id = cat.category_id
              WHERE
                  EventClass = 116 AND TEXTData LIKE '%SHRINK%' AND datediff(Minute, starttime, current_timestamp) < ?
              GROUP BY
                  databasename
          };
          @databaseresult = $params{handle}->fetchall_array($sql, $lookback);
        }
      }
      foreach my $name (@databasenames) {
        next if $params{database} && $name ne $params{database};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my $autogrowshrink = eval {
            map { $_->[1] } grep { $_->[0] eq $name } @databaseresult;
        } || 0;
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{growshrinkinterval} = $lookback;
        $thisparams{autogrowshrink} = $autogrowshrink;
        my $database = DBD::MSSQL::Server::Database->new(
            %thisparams);
        add_database($database);
        $num_databases++;
      }
      if (! $num_databases) {
        $initerrors = 1;
        return undef;
      }
    } elsif ($params{mode} =~ /server::database::.*backupage/) {
      my @databaseresult = ();
      if ($params{product} eq "MSSQL") {
        if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
          if ($params{mode} =~ /server::database::backupage/) {
            @databaseresult = $params{handle}->fetchall_array(q{
              SELECT D.name AS [database_name], D.recovery_model, BS1.last_backup, BS1.last_duration
              FROM sys.databases D
              LEFT JOIN (
                SELECT BS.[database_name],
                DATEDIFF(HH,MAX(BS.[backup_finish_date]),GETDATE()) AS last_backup,
                DATEDIFF(MI,MAX(BS.[backup_start_date]),MAX(BS.[backup_finish_date])) AS last_duration
                FROM msdb.dbo.backupset BS
                WHERE BS.type = 'D'
                GROUP BY BS.[database_name]
              ) BS1 ON D.name = BS1.[database_name]
              ORDER BY D.[name];
            });
          } elsif ($params{mode} =~ /server::database::logbackupage/) {
            @databaseresult = $params{handle}->fetchall_array(q{
              SELECT D.name AS [database_name], D.recovery_model, BS1.last_backup, BS1.last_duration
              FROM sys.databases D
              LEFT JOIN (
                SELECT BS.[database_name],
                DATEDIFF(HH,MAX(BS.[backup_finish_date]),GETDATE()) AS last_backup,
                DATEDIFF(MI,MAX(BS.[backup_start_date]),MAX(BS.[backup_finish_date])) AS last_duration
                FROM msdb.dbo.backupset BS
                WHERE BS.type = 'L'
                GROUP BY BS.[database_name]
              ) BS1 ON D.name = BS1.[database_name]
              ORDER BY D.[name];
            });
          }
        } else {
          @databaseresult = $params{handle}->fetchall_array(q{
            SELECT
              a.name, a.recovery_model,
              DATEDIFF(HH, MAX(b.backup_finish_date), GETDATE()),
              DATEDIFF(MI, MAX(b.backup_start_date), MAX(b.backup_finish_date))
            FROM master.dbo.sysdatabases a LEFT OUTER JOIN msdb.dbo.backupset b
            ON b.database_name = a.name
            GROUP BY a.name 
            ORDER BY a.name 
          }); 
        }
        foreach (sort {
          if (! defined $b->[1]) {
            return 1;
          } elsif (! defined $a->[1]) {
            return -1;
          } else {
            return $a->[1] <=> $b->[1];
          }
        } @databaseresult) { 
          my ($name, $recovery_model, $age, $duration) = @{$_};
          next if $params{database} && $name ne $params{database};
          if ($params{regexp}) { 
            next if $params{selectname} && $name !~ /$params{selectname}/;
          } else {
            next if $params{selectname} && lc $params{selectname} ne lc $name;
          }
          my %thisparams = %params;
          $thisparams{name} = $name;
          $thisparams{backup_age} = $age;
          $thisparams{backup_duration} = $duration;
          $thisparams{recovery_model} = $recovery_model;
          my $database = DBD::MSSQL::Server::Database->new(
              %thisparams);
          add_database($database);
          $num_databases++;
        }
      } elsif ($params{product} eq "ASE") {
        # sollte eigentlich als database::init implementiert werden, dann wiederum
        # gaebe es allerdings mssql=klassenmethode, ase=objektmethode. also hier.
        @databaseresult = $params{handle}->fetchall_array(q{
          SELECT name, dbid FROM master.dbo.sysdatabases
        });
        foreach (@databaseresult) {
          my ($name, $id) = @{$_};
          next if $params{database} && $name ne $params{database};
          if ($params{regexp}) {
            next if $params{selectname} && $name !~ /$params{selectname}/;
          } else {
            next if $params{selectname} && lc $params{selectname} ne lc $name;
          }
          my %thisparams = %params;
          $thisparams{name} = $name;
          $thisparams{id} = $id;
          $thisparams{backup_age} = undef;
          $thisparams{backup_duration} = undef;
          my $sql = q{
            dbcc traceon(3604)
            dbcc dbtable("?")
          };
          $sql =~ s/\?/$name/g;
          my @dbccresult = $params{handle}->fetchall_array($sql);
          foreach (@dbccresult) {
            #dbt_backup_start: 0x1686303d8 (dtdays=40599, dttime=7316475)    Feb 27 2011  6:46:28:250AM
            if (/dbt_backup_start: \w+\s+\(dtdays=0, dttime=0\) \(uninitialized\)/) {
              # never backed up
              last;
            } elsif (/dbt_backup_start: \w+\s+\(dtdays=\d+, dttime=\d+\)\s+(\w+)\s+(\d+)\s+(\d+)\s+(\d+):(\d+):(\d+):\d+([AP])/) {
              require Time::Local;
              my %months = ("Jan" => 0, "Feb" => 1, "Mar" => 2, "Apr" => 3, "May" => 4, "Jun" => 5, "Jul" => 6, "Aug" => 7, "Sep" => 8, "Oct" => 9, "Nov" => 10, "Dec" => 11);
              $thisparams{backup_age} = (time - Time::Local::timelocal($6, $5, $4 + ($7 eq "A" ? 0 : 12), $2, $months{$1}, $3 - 1900)) / 3600;
              $thisparams{backup_duration} = 0;
              last;
            }
          }
          # to keep compatibility with mssql. recovery_model=3=simple will be skipped later
          $thisparams{recovery_model} = 0;
          my $database = DBD::MSSQL::Server::Database->new(
              %thisparams);
          add_database($database);
          $num_databases++;
        }
        if (! $num_databases) {
          $initerrors = 1;
          return undef;
        }
      }
    }
  }
}

sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    handle => $params{handle},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    name => $params{name},
    id => $params{id},
    datafiles => [],
    backup_age => $params{backup_age},
    backup_duration => $params{backup_duration},
    autogrowshrink => $params{autogrowshrink},
    growshrinkinterval => $params{growshrinkinterval},
    state => $params{state},
    state_desc => lc $params{state_desc},
    collation_name => $params{collation_name},
    recovery_model => $params{recovery_model},
    offline => 0,
    accessible => 1,
    other_error => 0,
  };
  bless $self, $class;
  $self->init(%params);
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  # clear errors (offline, missing privileges...) from other databases
  $self->{handle}->{errstr} = "";
  $self->init_nagios();
  $self->set_local_db_thresholds(%params);
  if ($params{mode} =~ /server::database::datafile/) {
    $params{database} = $self->{name};
    DBD::MSSQL::Server::Database::Datafile::init_datafiles(%params);
    if (my @datafiles = 
        DBD::MSSQL::Server::Database::Datafile::return_datafiles()) {
      $self->{datafiles} = \@datafiles;
    } else {
      $self->add_nagios_critical("unable to aquire datafile info");
    }
  } elsif ($params{mode} =~ /server::database::databasefree/) {
    if (DBD::MSSQL::Server::return_first_server()->{product} eq "ASE") {
      # 0x0010 offline
      # 0x0020 offline until recovery completes
      $self->{offline} = $self->{state} & 0x0030;
    } elsif (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
      $self->{offline} = $self->{state} == 6 ? 1 : 0;
    } else {
      # bit 512 is offline
      $self->{offline} = $self->{state} & 0x0200;
    }
    ###################################################################################
    #                            fuer's museum
    # 1> sp_spaceused
    # 2> go
    # database_name   database_size   unallocated space
    # master  4.50 MB 1.32 MB
    # reserved        data    index_size      unused
    # 2744 KB 1056 KB 1064 KB 624 KB
    # (return status = 0)
    #my($database_name, $database_size, $unallocated_space,
    #    $reserved, $data, $index_size, $unused) =
    #     $params{handle}->fetchrow_array(
    #    "USE ".$self->{name}."\nEXEC SP_SPACEUSED"
    #);
    # server mgmt studio           sp_spaceused
    # Currently Allocated Space    database_size       641.94MB
    # Available Free Space         unallocated space   457.09MB 
    #$database_size =~ s/MB//g;
    #$unallocated_space =~ s/MB//g;
    #$self->{size} = $database_size * 1024 * 1024;
    #$self->{free} = $unallocated_space * 1024 * 1024;
    #$self->{percent_free} = $unallocated_space / $database_size * 100;
    #$self->{used} = $self->{size} - $self->{free};
    #$self->{maxsize} = "99999999999999999";
    ###################################################################################

    if (DBD::MSSQL::Server::return_first_server()->{product} eq "ASE") {
      my $cache_db_sizes = $params{cache_db_sizes}->{$self->{name}},
      my $maxpagesize = DBD::MSSQL::Server::return_first_server()->{maxpagesize};
      my $mb = 1024 * 1024;
      my($sp_db_name, $sp_db_size, $sp_db_reserved, $sp_db_data, $sp_db_index, $sp_db_unused) =
           $params{handle}->fetchrow_array(
          "USE ".$self->{name}."\nEXEC sp_spaceused"
      );
      $sp_db_reserved =~ /([\d\.]+)\s*([GMKB]+)/;
      $sp_db_reserved = $1 * ($2 eq "KB" ? 1/1024 : ($2 eq "GB" ? 1024 : 1));
      $sp_db_data =~ /([\d\.]+)\s*([GMKB]+)/;
      $sp_db_data = $1 * ($2 eq "KB" ? 1/1024 : ($2 eq "GB" ? 1024 : 1));
      $sp_db_index =~ /([\d\.]+)\s*([GMKB]+)/;
      $sp_db_index = $1 * ($2 eq "KB" ? 1/1024 : ($2 eq "GB" ? 1024 : 1));
      $sp_db_unused =~ /([\d\.]+)\s*([GMKB]+)/;
      $sp_db_unused = $1 * ($2 eq "KB" ? 1/1024 : ($2 eq "GB" ? 1024 : 1));
      my($sp_log_name, $sp_log_total_pages, $sp_log_free_pages, $sp_log_used_pages, $sp_log_reserved_pages) =
          # returns number of logical pages
          $params{handle}->fetchrow_array(
        "USE ".$self->{name}."\nEXEC sp_spaceused syslogs"
      );
      $sp_log_total_pages = $sp_log_total_pages * $maxpagesize / $mb;  # ! name is _pages, but unit is mb
      $sp_log_free_pages = $sp_log_free_pages * $maxpagesize / $mb;
      $sp_log_used_pages = $sp_log_used_pages * $maxpagesize / $mb;
      $sp_log_reserved_pages = $sp_log_reserved_pages * $maxpagesize / $mb;

      my $unreserved = $cache_db_sizes->{data_size} - $sp_db_reserved;
      my $reserved = $sp_db_reserved;
      my $unreserved_pct = 100 * ($unreserved / $cache_db_sizes->{data_size});
      my $reserved_pct = 100  - $unreserved_pct;

      if ($reserved_pct < 0 || $reserved_pct > 100) {
        # werte aus sp_spaceused
        # sind von der theorie her nicht so exakt, in der praxis aber doch.
        # wenn obige werte seltsam aussehen, dann lieber daten aus sysusages verwenden.
        $unreserved = $cache_db_sizes->{data_free}; # 
        $reserved = $cache_db_sizes->{data_size} - $unreserved;
        $unreserved_pct = 100 * ($unreserved / $cache_db_sizes->{data_size});
        $reserved_pct = 100  - $unreserved_pct;
      }
      if ($cache_db_sizes->{log_size}) {
        # has separate transaction log devices
        my $database_size = $cache_db_sizes->{data_size} + $cache_db_sizes->{log_size};
        my $log_free = $cache_db_sizes->{log_free};
        my $log_used = $cache_db_sizes->{log_used};
        my $log_free_pct = 100 * ($sp_log_free_pages / $sp_log_total_pages);
        my $log_used_pct = 100 - $log_free_pct;
        if ($sp_log_reserved_pages < 0 && ($log_free_pct < 0 || $log_free_pct > 100)) {
          # sp_spaceused data are not good enough, need to run dbcc
          $log_free_pct = 100 * ($log_free / $cache_db_sizes->{log_size});
          $log_used_pct = 100 - $log_free_pct;
        }
        $self->{max_mb} = $cache_db_sizes->{data_size};
        $self->{free_mb} = $cache_db_sizes->{data_free};
        $self->{free_percent} = $unreserved_pct;
        $self->{allocated_percent} = $reserved_pct; # data_size - unreserved - unused
        $self->{free_log_mb} = $sp_log_free_pages;
        $self->{free_log_percent} = $log_free_pct;
        #printf "%14s %.2f%% %.2f%%  %.2f%%  %.2f%%\n", 
        #    $self->{name}, $unreserved_pct, $reserved_pct, $log_free_pct, $log_used_pct;
      } else {
        my $database_size = $cache_db_sizes->{data_size};

        my $log_used_pct = 100 * $sp_log_used_pages / $cache_db_sizes->{data_size};
        my $log_free_pct = 100 * $sp_log_free_pages / $cache_db_sizes->{data_size};
        #printf "%14s %.2f%% %.2f%%  %.2f%%  %.2f%%\n", 
        #    $self->{name}, $unreserved_pct, $reserved_pct, $log_free_pct, $log_used_pct;
        $self->{max_mb} = $cache_db_sizes->{data_size};
        $self->{free_mb} = $unreserved;
        $self->{free_percent} = $unreserved_pct;
        $self->{allocated_percent} = $reserved_pct; # data_size - unreserved - unused
        $self->{free_log_mb} = $sp_log_free_pages;
        $self->{free_log_percent} = $log_free_pct;
      }
    } else {
      my $calc = {};
      if ($params{method} eq 'sqlcmd' || $params{method} eq 'sqsh') {
        foreach($self->{handle}->fetchall_array(q{
          if object_id('tempdb..#FreeSpace') is null
            create table #FreeSpace(
              Drive varchar(10),
              MB_Free bigint
            )
          go
          DELETE FROM tempdb..#FreeSpace
          INSERT INTO tempdb..#FreeSpace exec master.dbo.xp_fixeddrives
          go
          SELECT * FROM tempdb..#FreeSpace
        })) {
          $calc->{drive_mb}->{lc $_->[0]} = $_->[1];
        }
      } else {
        $self->{handle}->execute(q{
          if object_id('tempdb..#FreeSpace') is null 
            create table #FreeSpace(  
              Drive varchar(10),  
              MB_Free bigint  
            ) 
        });
        $self->{handle}->execute(q{
          DELETE FROM #FreeSpace
        });
        $self->{handle}->execute(q{
          INSERT INTO #FreeSpace exec master.dbo.xp_fixeddrives
        });
        foreach($self->{handle}->fetchall_array(q{
          SELECT * FROM #FreeSpace
        })) {
          $calc->{drive_mb}->{lc $_->[0]} = $_->[1];
        }
      }
      #$self->{handle}->execute(q{
      #  DROP TABLE #FreeSpace
      #});
      # Page = 8KB
      # sysfiles ist sv2000, noch als kompatibilitaetsview vorhanden
      # dbo.sysfiles kann 2008 durch sys.database_files ersetzt werden?
      # omeiomeiomei in 2005 ist ein sys.sysindexes compatibility view
      #   fuer 2000.dbo.sysindexes
      #   besser ist sys.allocation_units
      if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
        my $sql = q{
            SELECT 
                SUM(CAST(used AS BIGINT)) / 128
            FROM 
                [?].sys.sysindexes
            WHERE
                indid IN (0,1,255)
        };
        #$sql =~ s/\[\?\]/$self->{name}/g;
        $sql =~ s/\?/$self->{name}/g;
        $self->{used_mb} = $self->{handle}->fetchrow_array($sql);
        my $retries = 0;
        while ($retries < 10 && $self->{handle}->{errstr} =~ /was deadlocked .* victim/i) {
          # Sachen gibt's.... DBD::Sybase::st execute failed: Server message number=1205 severity=13 state=56 line=2 server=AUDIINSV0665 text=Transaction (Process ID 173) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction.
          sleep 0.1;
          $self->trace(sprintf
              "%s sysindexes query was a deadlock victim. retry",
              $self->{name});
          $self->{used_mb} = $self->{handle}->fetchrow_array($sql);
          $retries++;
        }
      } else {
        my $sql = q{
            SELECT 
                SUM(CAST(used AS BIGINT)) / 128
            FROM 
                [?].dbo.sysindexes
            WHERE
                indid IN (0,1,255)
        };
        #$sql =~ s/\[\?\]/$self->{name}/g;
        $sql =~ s/\?/$self->{name}/g;
        $self->{used_mb} = $self->{handle}->fetchrow_array($sql);
      }
      if (exists $self->{used_mb} && ! defined $self->{used_mb}) {
        # exists = Query ist gelaufen
        # ! defined = hat NULL geliefert (Ja, gibt's. ich habe es gerade
        # mit eigenen Augen gesehen)
        $self->{used_mb} = 0;
        $self->trace(sprintf "%s uses no indices", $self->{name});
        $self->trace(sprintf "also error %s", $self->{handle}->{errstr}) if $self->{handle}->{errstr};

      }
      my @fileresult = ();
      if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
        my $sql = q{
            SELECT
                RTRIM(a.name), RTRIM(a.filename), CAST(a.size AS BIGINT),
                CAST(a.maxsize AS BIGINT), a.growth
            FROM
                [?].sys.sysfiles a
            JOIN
                [?].sys.sysfilegroups b
            ON
                a.groupid = b.groupid
        };
        #$sql =~ s/\[\?\]/$self->{name}/g;
        $sql =~ s/\?/$self->{name}/g;
        @fileresult = $self->{handle}->fetchall_array($sql);
        if ($self->{handle}->{errstr} =~ /offline/i) {
          $self->{allocated_mb} = 0;
          $self->{max_mb} = 1;
          $self->{used_mb} = 0;
        } elsif ($self->{handle}->{errstr} =~ /is not able to access the database/i) {
          $self->{accessible} = 0;
          $self->{allocated_mb} = 0;
          $self->{max_mb} = 1;
          $self->{used_mb} = 0;
        } elsif ($self->{handle}->{errstr}) {
          $self->{allocated_mb} = 0;
          $self->{max_mb} = 1;
          $self->{used_mb} = 0;
          if ($self->{handle}->{errstr} =~ /Message String: ([\w ]+)/) {
            $self->{other_error} = $1;
          } else {
            $self->{other_error} = $self->{handle}->{errstr};
          }
        }
      } else {
        my $sql = q{
            SELECT
                RTRIM(a.name), RTRIM(a.filename), CAST(a.size AS BIGINT),
                CAST(a.maxsize AS BIGINT), a.growth
            FROM
                [?].dbo.sysfiles a
            JOIN
                [?].dbo.sysfilegroups b
            ON
                a.groupid = b.groupid
        };
        #$sql =~ s/\[\?\]/$self->{name}/g;
        $sql =~ s/\?/$self->{name}/g;
        @fileresult = $self->{handle}->fetchall_array($sql);
      }
      foreach(@fileresult) {
        my($name, $filename, $size, $maxsize, $growth) = @{$_};
        my $drive = lc substr($filename, 0, 1);
        $calc->{datafile}->{$name}->{allocsize} = $size / 128;
        if ($growth == 0) {
          $calc->{datafile}->{$name}->{maxsize} = $size / 128;
        } else {
          if ($maxsize == -1) {
            $calc->{datafile}->{$name}->{maxsize} =
                exists $calc->{drive_mb}->{$drive} ?
                    ($calc->{datafile}->{$name}->{allocsize} + 
                     $calc->{drive_mb}->{$drive}) : 4 * 1024;
            # falls die platte nicht gefunden wurde, dann nimm halt 4GB
            if (exists $calc->{drive_mb}->{$drive}) {
              # davon kann ausgegangen werden. wenn die drives nicht zur
              # vefuegung stehen, stimmt sowieso hinten und vorne nichts.
              $calc->{drive_mb}->{$drive} = 0;
              # damit ist der platz dieses laufwerks verbraten und in
              # max_mb eingeflossen. es darf nicht sein, dass der freie platz
              # mehrfach gezaehlt wird, wenn es mehrere datafiles auf diesem
              # laufwerk gibt.
            }
          } else {
            $calc->{datafile}->{$name}->{maxsize} = $maxsize / 128;
          }
        }
        $self->{allocated_mb} += $calc->{datafile}->{$name}->{allocsize};
        $self->{max_mb} += $calc->{datafile}->{$name}->{maxsize};
      }
      $self->{allocated_mb} = $self->{allocated_mb};
      if ($self->{used_mb} > $self->{allocated_mb}) {
        # obige used-berechnung liefert manchmal (wenns knapp hergeht) mehr als
        # den maximal verfuegbaren platz. vermutlich muessen dann
        # zwecks ermittlung des tatsaechlichen platzverbrauchs 
        # irgendwelche dbcc updateusage laufen.
        # egal, wird schon irgendwie stimmen.
        $self->{used_mb} = $self->{allocated_mb};
        $self->{estimated} = 1;
      } else {
        $self->{estimated} = 0;
      }
      $self->{free_mb} = $self->{max_mb} - $self->{used_mb};
      $self->{free_percent} = 100 * $self->{free_mb} / $self->{max_mb};
      $self->{allocated_percent} = 100 * $self->{allocated_mb} / $self->{max_mb};
    }
  } elsif ($params{mode} =~ /^server::database::transactions/) {
    $self->{transactions_s} = $self->{handle}->get_perf_counter_instance(
        'SQLServer:Databases', 'Transactions/sec', $self->{name});
    if (! defined $self->{transactions_s}) {
      $self->add_nagios_unknown("unable to aquire counter data");
    } else {
      $self->valdiff(\%params, qw(transactions_s));
      $self->{transactions_per_sec} = $self->{delta_transactions_s} / $self->{delta_timestamp};
    }
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::database::datafile::listdatafiles/) {
      foreach (sort { $a->{logicalfilename} cmp $b->{logicalfilename}; }  @{$self->{datafiles}}) {
	printf "%s\n", $_->{logicalfilename};
      }
      $self->add_nagios_ok("have fun");
    } elsif ($params{mode} =~ /server::database::online/) {
      if ($self->{state_desc} eq "online") {
        if ($self->{collation_name}) {
          $self->add_nagios_ok(
            sprintf "%s is %s and accepting connections", $self->{name}, $self->{state_desc});
        } else {
          $self->add_nagios_warning(
            sprintf "%s is %s but not accepting connections", $self->{name}, $self->{state_desc});
        }
      } elsif ($self->{state_desc} =~ /^recover/) {
        $self->add_nagios_warning(
            sprintf "%s is %s", $self->{name}, $self->{state_desc});
      } else {
        $self->add_nagios_critical(
            sprintf "%s is %s", $self->{name}, $self->{state_desc});
      }
    } elsif ($params{mode} =~ /^server::database::transactions/) {
      $self->add_nagios(
          $self->check_thresholds($self->{transactions_per_sec}, 10000, 50000),
          sprintf "%s has %.4f transactions / sec",
          $self->{name}, $self->{transactions_per_sec});
      $self->add_perfdata(sprintf "%s_transactions_per_sec=%.4f;%s;%s",
          $self->{name}, $self->{transactions_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /server::database::databasefree/) {
      # ->percent_free
      # ->free
      #
      # ausgabe
      #   perfdata db_<db>_free_pct
      #   perfdata db_<db>_free        (real_bytes_max - bytes) + bytes_free  (with units)
      #   perfdata db_<db>_alloc_free  bytes_free (with units)
      #
              # umrechnen der thresholds
      # ()/%
      # MB
      # GB
      # KB
      if (($self->{warningrange} && $self->{warningrange} !~ /^\d+[\.\d]*:/) ||
          ($self->{criticalrange} && $self->{criticalrange} !~ /^\d+[\.\d]*:/)) {
        $self->add_nagios_unknown("you want an alert if free space is _above_ a threshold????");
        return;
      }
      if (! $params{units}) {
        $params{units} = "%";
      } else {
        $params{units} = uc $params{units};
      }
      $self->{warning_bytes} = 0;
      $self->{critical_bytes} = 0;
      if ($self->{offline}) {
        # offlineok hat vorrang
        $params{mitigation} = $params{offlineok} ? 0 : defined $params{mitigation} ? $params{mitigation} : 1;
        $self->add_nagios(
            $params{mitigation},
            sprintf("database %s is offline", $self->{name})
        );
      } elsif (! $self->{accessible}) {
        $self->add_nagios(
            defined $params{mitigation} ? $params{mitigation} : 1, 
            sprintf("insufficient privileges to access %s", $self->{name})
        );
      } elsif ($self->{other_error}) {
        $self->add_nagios(
            defined $params{mitigation} ? $params{mitigation} : 1, 
            sprintf("error accessing %s: %s", $self->{name}, $self->{other_error})
        );
      } elsif ($params{units} eq "%") {
        $self->add_nagios(
            $self->check_thresholds($self->{free_percent}, "5:", "2:"),
                sprintf("database %s has %.2f%% free space left",
                $self->{name}, $self->{free_percent},
                ($self->{estimated} ? " (estim.)" : ""))
        );
        $self->add_perfdata(sprintf "\'db_%s_free_pct\'=%.2f%%;%s;%s",
            lc $self->{name},
            $self->{free_percent},
            $self->{warningrange}, $self->{criticalrange});
        $self->add_perfdata(sprintf "\'db_%s_free\'=%.2fMB;%.2f:;%.2f:;0;%.2f",
            lc $self->{name},
            $self->{free_mb},
            (($self->{warn_mb} = $self->{warningrange}) =~ s/://g && $self->{warn_mb} || $self->{warningrange}) * $self->{max_mb} / 100,
            (($self->{crit_mb} = $self->{criticalrange}) =~ s/://g && $self->{crit_mb} || $self->{criticalrange}) * $self->{max_mb} / 100,
            $self->{max_mb});
        $self->add_perfdata(sprintf "\'db_%s_allocated_pct\'=%.2f%%",
            lc $self->{name},
            $self->{allocated_percent});
        if (exists $self->{free_log_percent}) {
          # sybase with extra transaction log device
          $self->add_nagios(
              $self->check_thresholds($self->{free_log_percent}, "5:", "2:"),
                  sprintf("database %s has %.2f%% free log space left",
                  $self->{name}, $self->{free_log_percent},
                  ($self->{estimated} ? " (estim.)" : ""))
          );
          $self->add_perfdata(sprintf "\'db_%s_free_log_pct\'=%.2f%%;%s;%s",
              lc $self->{name},
              $self->{free_log_percent},
              $self->{warningrange}, $self->{criticalrange});
        }
      } else {
        my $factor = 1; # default MB
        if ($params{units} eq "GB") {
          $factor = 1024;
        } elsif ($params{units} eq "MB") {
          $factor = 1;
        } elsif ($params{units} eq "KB") {
          $factor = 1 / 1024;
        }
        $self->{warningrange} ||= "5:";
        $self->{criticalrange} ||= "2:";
        # : entfernen weil gerechnet werden muss
        my $wnum = $self->{warningrange};
        my $cnum = $self->{criticalrange};
        $wnum =~ s/://g;
        $cnum =~ s/://g;
        $wnum *= $factor; # ranges in mb umrechnen
        $cnum *= $factor;
        $self->{percent_warning} = 100 * $wnum / $self->{max_mb};
        $self->{percent_critical} = 100 * $wnum / $self->{max_mb};
        #$self->{warningrange} = ($wnum / $factor).":";
        #$self->{criticalrange} = ($cnum / $factor).":";
        $self->add_nagios(
            $self->check_thresholds($self->{free_mb} / $factor, "5242880:", "1048576:"),
                sprintf("database %s has %.2f%s free space left", $self->{name},
                    $self->{free_mb} / $factor, $params{units})
        );
        $self->add_perfdata(sprintf "\'db_%s_free_pct\'=%.2f%%;%.2f:;%.2f:",
            lc $self->{name},
            $self->{free_percent}, $self->{percent_warning},
            $self->{percent_critical});
        $self->add_perfdata(sprintf "\'db_%s_free\'=%.2f%s;%s;%s;0;%.2f",
            lc $self->{name},
            $self->{free_mb} / $factor, $params{units},
            $self->{warningrange},
            $self->{criticalrange},
            $self->{max_mb} / $factor);
        $self->add_perfdata(sprintf "\'db_%s_allocated_pct\'=%.2f%%",
            lc $self->{name},
            $self->{allocated_percent});
        if (exists $self->{free_log_percent}) {
          # sybase with extra transaction log device
          $self->add_nagios(
              $self->check_thresholds($self->{free_log_mb} / $factor, "5:", "2:"),
                  sprintf("database %s has %.2f%s free log space left",
                  $self->{name}, $self->{free_log_mb} / $factor, $params{units})
          );
          $self->add_perfdata(sprintf "\'db_%s_free_log_pct\'=%.2f%%;%s;%s",
              lc $self->{name},
              $self->{free_log_percent},
              $self->{warningrange}, $self->{criticalrange});
        }
      }
    } elsif ($params{mode} =~ /server::database::auto(growths|shrinks)/) {
      my $type = ""; 
      if ($params{mode} =~ /::datafile/) {
        $type = "data ";
      } elsif ($params{mode} =~ /::logfile/) {
        $type = "log ";
      }
      $self->add_nagios( 
          $self->check_thresholds($self->{autogrowshrink}, 1, 5), 
          sprintf "%s had %d %sfile auto %s events in the last %d minutes", $self->{name},
              $self->{autogrowshrink}, $type, 
              ($params{mode} =~ /server::database::autogrowths/) ? "grow" : "shrink",
              $self->{growshrinkinterval});
    } elsif ($params{mode} =~ /server::database::dbccshrinks/) {
      # nur relevant fuer master
      $self->add_nagios( 
          $self->check_thresholds($self->{autogrowshrink}, 1, 5), 
          sprintf "%s had %d DBCC Shrink events in the last %d minutes", $self->{name}, $self->{autogrowshrink}, $self->{growshrinkinterval});
    } elsif ($params{mode} =~ /server::database::.*backupage/) {
      my $log = "";
      if ($params{mode} =~ /server::database::logbackupage/) {
        $log = "log of ";
      }
      if ($params{mode} =~ /server::database::logbackupage/ &&
          $self->{recovery_model} == 3) {
        $self->add_nagios_ok(sprintf "%s has no logs",
            $self->{name}); 
      } else {
        if (! defined $self->{backup_age}) { 
          $self->add_nagios(defined $params{mitigation} ? $params{mitigation} : 2, sprintf "%s%s was never backed up",
              $log, $self->{name}); 
          $self->{backup_age} = 0;
          $self->{backup_duration} = 0;
          $self->check_thresholds($self->{backup_age}, 48, 72); # init wg perfdata
        } else { 
          $self->add_nagios( 
              $self->check_thresholds($self->{backup_age}, 48, 72), 
              sprintf "%s%s was backed up %dh ago", $log, $self->{name}, $self->{backup_age});
        } 
        $self->add_perfdata(sprintf "'%s_bck_age'=%d;%s;%s", 
            $self->{name}, $self->{backup_age}, 
            $self->{warningrange}, $self->{criticalrange}); 
        $self->add_perfdata(sprintf "'%s_bck_time'=%d", 
            $self->{name}, $self->{backup_duration}); 
      }
    } 
  }
}



package DBD::MSSQL::Server::Job;

use strict;

our @ISA = qw(DBD::MSSQL::Server);


{
  my @jobs = ();
  my $initerrors = undef;

  sub add_job {
    push(@jobs, shift);
  }

  sub return_jobs {
    return reverse
        sort { $a->{name} cmp $b->{name} } @jobs;
  }

  sub init_jobs {
    my %params = @_;
    my $num_jobs = 0;
    if (($params{mode} =~ /server::jobs::failed/) ||
        ($params{mode} =~ /server::jobs::dummy/)) {
      my @jobresult = ();
      if ($params{product} eq "MSSQL") {
        if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
          @jobresult = $params{handle}->fetchall_array(q{
            SELECT 
                [sJOB].[job_id] AS [JobID]
                , [sJOB].[name] AS [JobName]
                ,CURRENT_TIMESTAMP  --can be used for debugging
                , CASE 
                    WHEN [sJOBH].[run_date] IS NULL OR [sJOBH].[run_time] IS NULL THEN NULL
                    ELSE datediff(Minute, CAST(
                        CAST([sJOBH].[run_date] AS CHAR(8))
                        + ' ' 
                        + STUFF(
                            STUFF(RIGHT('000000' + CAST([sJOBH].[run_time] AS VARCHAR(6)),  6)
                                , 3, 0, ':')
                                , 6, 0, ':')
                        AS DATETIME), current_timestamp)
                  END AS [MinutesSinceStart]
                ,CAST(SUBSTRING(RIGHT('000000' + CAST([sJOBH].[run_duration] AS VARCHAR(6)), 6), 1, 2) AS INT) * 3600 +
                 CAST(SUBSTRING(RIGHT('000000' + CAST([sJOBH].[run_duration] AS VARCHAR(6)), 6), 3, 2) AS INT) * 60 +
                 CAST(SUBSTRING(RIGHT('000000' + CAST([sJOBH].[run_duration] AS VARCHAR(6)), 6), 5, 2) AS INT) AS LastRunDurationSeconds
                , CASE 
                    WHEN [sJOBH].[run_date] IS NULL OR [sJOBH].[run_time] IS NULL THEN NULL
                    ELSE CAST(
                            CAST([sJOBH].[run_date] AS CHAR(8))
                            + ' ' 
                            + STUFF(
                                STUFF(RIGHT('000000' + CAST([sJOBH].[run_time] AS VARCHAR(6)),  6)
                                    , 3, 0, ':')
                                , 6, 0, ':')
                            AS DATETIME)
                  END AS [LastRunDateTime]
                , CASE [sJOBH].[run_status]
                    WHEN 0 THEN 'Failed'
                    WHEN 1 THEN 'Succeeded'
                    WHEN 2 THEN 'Retry'
                    WHEN 3 THEN 'Canceled'
                    WHEN 4 THEN 'Running' -- In Progress
                  END AS [LastRunStatus]
                , STUFF(
                        STUFF(RIGHT('000000' + CAST([sJOBH].[run_duration] AS VARCHAR(6)),  6)
                            , 3, 0, ':')
                        , 6, 0, ':') 
                    AS [LastRunDuration (HH:MM:SS)]
                , [sJOBH].[message] AS [LastRunStatusMessage]
                , CASE [sJOBSCH].[NextRunDate]
                    WHEN 0 THEN NULL
                    ELSE CAST(
                            CAST([sJOBSCH].[NextRunDate] AS CHAR(8))
                            + ' ' 
                            + STUFF(
                                STUFF(RIGHT('000000' + CAST([sJOBSCH].[NextRunTime] AS VARCHAR(6)),  6)
                                    , 3, 0, ':')
                                , 6, 0, ':')
                            AS DATETIME)
                  END AS [NextRunDateTime]
            FROM 
                [msdb].[dbo].[sysjobs] AS [sJOB]
                LEFT JOIN (
                            SELECT
                                [job_id]
                                , MIN([next_run_date]) AS [NextRunDate]
                                , MIN([next_run_time]) AS [NextRunTime]
                            FROM [msdb].[dbo].[sysjobschedules]
                            GROUP BY [job_id]
                        ) AS [sJOBSCH]
                    ON [sJOB].[job_id] = [sJOBSCH].[job_id]
                LEFT JOIN (
                            SELECT 
                                [job_id]
                                , [run_date]
                                , [run_time]
                                , [run_status]
                                , [run_duration]
                                , [message]
                                , ROW_NUMBER() OVER (
                                                        PARTITION BY [job_id] 
                                                        ORDER BY [run_date] DESC, [run_time] DESC
                                  ) AS RowNumber
                            FROM [msdb].[dbo].[sysjobhistory]
                            WHERE [step_id] = 0
                        ) AS [sJOBH]
                    ON [sJOB].[job_id] = [sJOBH].[job_id]
                    AND [sJOBH].[RowNumber] = 1
            ORDER BY [JobName]
          });
        } else {
          @jobresult = ();
        }
      } elsif ($params{product} eq "ASE") {
        @jobresult = $params{handle}->fetchall_array(q{
          SELECT name, dbid FROM master.dbo.sysjobs
        });
      }
      foreach (@jobresult) {
        my ($id, $name, $now, $minutessincestart, $lastrundurationseconds, $lastrundatetime, $lastrunstatus, $lastrunduration, $lastrunstatusmessage, $nextrundatetime) = @{$_};
        next if $minutessincestart > $params{lookback};
        next if $params{job} && $name ne $params{job};
        if ($params{regexp}) {
          next if $params{selectname} && $name !~ /$params{selectname}/;
        } else {
          next if $params{selectname} && lc $params{selectname} ne lc $name;
        }
        my %thisparams = %params;
        $thisparams{name} = $name;
        $thisparams{id} = $id;
        $thisparams{lastrundatetime} = $lastrundatetime;
        $thisparams{minutessincestart} = $minutessincestart;
        $thisparams{lastrundurationseconds} = $lastrundurationseconds;
        $thisparams{lastrunstatus} = $lastrunstatus;
        $thisparams{lastrunstatusmessage} = $lastrunstatusmessage;
        my $job = DBD::MSSQL::Server::Job->new(
            %thisparams);
        add_job($job);
        $num_jobs++;
      }
      if (! $num_jobs) {
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
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    name => $params{name},
    id => $params{id},
    lastrundatetime => $params{lastrundatetime},
    minutessincestart => $params{minutessincestart},
    lastrundurationseconds => $params{lastrundurationseconds},
    lastrunstatus => lc $params{lastrunstatus},
    lastrunstatusmessage => $params{lastrunstatusmessage},
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
  if ($params{mode} =~ /server::jobs::failed/) {
    #printf "init job %s\n", Data::Dumper::Dumper($self);
  }
}

sub nagios {
  my $self = shift;
  my %params = @_;
  if (! $self->{nagios_level}) {
    if ($params{mode} =~ /server::jobs::failed/) {
      if ($self->{lastrunstatus} eq "failed") {
          $self->add_nagios_critical(
              sprintf "%s failed: %s", $self->{name}, $self->{lastrunstatusmessage});
      } elsif ($self->{lastrunstatus} eq "retry" || $self->{lastrunstatus} eq "canceled") {
          $self->add_nagios_warning(
              sprintf "%s %s: %s", $self->{name}, $self->{lastrunstatus}, $self->{lastrunstatusmessage});
      } else {
        $self->add_nagios(
            $self->check_thresholds($self->{lastrundurationseconds}, 60, 300),
                sprintf("job %s ran for %d seconds (started %s)", $self->{name}, 
                $self->{lastrundurationseconds}, $self->{lastrundatetime}));
      }
    } 
  }
}



package DBD::MSSQL::Server;

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
    username => $params{username},
    password => $params{password},
    port => $params{port} || 1433,
    server => $params{server},
    timeout => $params{timeout},
    warningrange => $params{warningrange},
    criticalrange => $params{criticalrange},
    verbose => $params{verbose},
    report => $params{report},
    version => 'unknown',
    os => 'unknown',
    servicename => 'unknown',
    instance => undef,
    memorypool => undef,
    databases => [],
    handle => undef,
  };
  bless $self, $class;
  $self->init_nagios();
  if ($self->dbconnect(%params)) {
    #$self->{version} = $self->{handle}->fetchrow_array(
    #    q{ SELECT SERVERPROPERTY('productversion') });
    # @@VERSION:
    # Variant1:
    # Adaptive Server Enterprise/15.5/EBF 18164 SMP ESD#2/P/x86_64/Enterprise Linux/asear155/2514/64-bit/FBO/Wed Aug 25 11:17:26 2010
    # Variant2: 
    # Microsoft SQL Server 2005 - 9.00.1399.06 (Intel X86)
    #    Oct 14 2005 00:33:37
    #    Copyright (c) 1988-2005 Microsoft Corporation
    #    Enterprise Edition on Windows NT 5.2 (Build 3790: Service Pack 2)

    map {
        $self->{os} = "Linux" if /Linux/;
        $self->{version} = $1 if /Adaptive Server Enterprise\/([\d\.]+)/;
        $self->{os} = $1 if /Windows (.*)/;
        $self->{version} = $1 if /SQL Server.*\-\s*([\d\.]+)/;
        $self->{product} = "ASE" if /Adaptive Server/;
        $self->{product} = "MSSQL" if /SQL Server/;
    } $self->{handle}->fetchrow_array(
        q{ SELECT @@VERSION });
    # params wird tiefer weitergereicht, z.b. zu databasefree
    $params{product} = $self->{product};
    if ($self->{product} eq "MSSQL") {
        $self->{dbuser} = $self->{handle}->fetchrow_array(
            q{ SELECT SYSTEM_USER });  # maybe SELECT SUSER_SNAME()
      $self->{servicename} = $self->{handle}->fetchrow_array(
          q{ SELECT @@SERVICENAME });
      if (lc $self->{servicename} ne 'mssqlserver') {
        # braucht man fuer abfragen von dm_os_performance_counters
        # object_name ist entweder "SQLServer:Buffer Node" oder z.b. "MSSQL$OASH:Buffer Node"
        $self->{servicename} = 'MSSQL$'.$self->{servicename};
      } else {
        $self->{servicename} = 'SQLServer';
      }
    } else {
        $self->{dbuser} = $self->{handle}->fetchrow_array(
            q{ SELECT SUSER_NAME() });
        $self->{maxpagesize} = $self->{handle}->fetchrow_array(
            q{ SELECT @@MAXPAGESIZE });
    }
    DBD::MSSQL::Server::add_server($self);
    $self->init(%params);
  }
  return $self;
}

sub init {
  my $self = shift;
  my %params = @_;
  $params{handle} = $self->{handle};
  $self->set_global_db_thresholds(\%params);
  if ($params{mode} =~ /^server::memorypool/) {
    $self->{memorypool} = DBD::MSSQL::Server::Memorypool->new(%params);
  } elsif ($params{mode} =~ /^server::database/) {
    DBD::MSSQL::Server::Database::init_databases(%params);
    if (my @databases =
        DBD::MSSQL::Server::Database::return_databases()) {
      $self->{databases} = \@databases;
    } else {
      $self->add_nagios_critical("unable to aquire database info");
    }
  } elsif ($params{mode} =~ /^server::jobs/) {
    DBD::MSSQL::Server::Job::init_jobs(%params);
    if (my @jobs =
        DBD::MSSQL::Server::Job::return_jobs()) {
      $self->{jobs} = \@jobs;
    } else {
      $self->add_nagios_critical(sprintf "no jobs ran within the last %d minutes", $params{lookback});
    }
  } elsif ($params{mode} =~ /^server::connectiontime/) {
    $self->{connection_time} = $self->{tac} - $self->{tic};
  } elsif ($params{mode} =~ /^server::cpubusy/) {
    if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
      #($self->{secs_busy}) = $self->{handle}->fetchrow_array(q{
      #    SELECT ((@@CPU_BUSY * CAST(@@TIMETICKS AS FLOAT)) /
      #        (SELECT (CAST(CPU_COUNT AS FLOAT) / CAST(HYPERTHREAD_RATIO AS FLOAT)) FROM sys.dm_os_sys_info) /
      #        1000000)
      #});
      # new: people were complaining about percentages > 100%
      # http://sqlblog.com/blogs/kalen_delaney/archive/2007/12/08/hyperthreaded-or-not.aspx
      # count only cpus, virtual or not, cores or threads
      # this is some debugging code to see the single values with -v
      my $cpu_busy = $self->{handle}->fetchrow_array(q{
          SELECT @@CPU_BUSY FROM sys.dm_os_sys_info
      });
      my $timeticks = $self->{handle}->fetchrow_array(q{
          SELECT CAST(@@TIMETICKS AS FLOAT) FROM sys.dm_os_sys_info
      });
      my $cpu_count = $self->{handle}->fetchrow_array(q{
          SELECT CAST(CPU_COUNT AS FLOAT) FROM sys.dm_os_sys_info
      });
      my $hyperthread_ratio = $self->{handle}->fetchrow_array(q{
          SELECT CAST(HYPERTHREAD_RATIO AS FLOAT) FROM sys.dm_os_sys_info
      });
      ($self->{secs_busy}) = $self->{handle}->fetchrow_array(q{
          SELECT ((@@CPU_BUSY * CAST(@@TIMETICKS AS FLOAT)) /
              (SELECT (CAST(CPU_COUNT AS FLOAT)) FROM sys.dm_os_sys_info) /
              1000000)
      });
      $self->valdiff(\%params, qw(secs_busy));
      if (defined $self->{secs_busy}) {
        $self->{cpu_busy} = 100 *
            $self->{delta_secs_busy} / $self->{delta_timestamp};
      } else {
        $self->add_nagios_critical("got no cputime from dm_os_sys_info");
      }
    } else {
      #$self->requires_version('9');
      my @monitor = $params{handle}->exec_sp_1hash(q{exec sp_monitor});
      foreach (@monitor) {
        if ($_->[0] eq 'cpu_busy') {
          if ($_->[1] =~ /(\d+)%/) {
            $self->{cpu_busy} = $1;
          }
        }
      }
      self->requires_version('9') unless defined $self->{cpu_busy};
    }
  } elsif ($params{mode} =~ /^server::iobusy/) {
    if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
      #($self->{secs_busy}) = $self->{handle}->fetchrow_array(q{
      #    SELECT ((@@IO_BUSY * CAST(@@TIMETICKS AS FLOAT)) /
      #        (SELECT (CAST(CPU_COUNT AS FLOAT) / CAST(HYPERTHREAD_RATIO AS FLOAT)) FROM sys.dm_os_sys_info) /
      #        1000000)
      #});
      ($self->{secs_busy}) = $self->{handle}->fetchrow_array(q{
          SELECT ((@@IO_BUSY * CAST(@@TIMETICKS AS FLOAT)) /
              (SELECT CAST(CPU_COUNT AS FLOAT) FROM sys.dm_os_sys_info) /
              1000000)
      });
      $self->valdiff(\%params, qw(secs_busy));
      if (defined $self->{secs_busy}) {
        $self->{io_busy} = 100 *
            $self->{delta_secs_busy} / $self->{delta_timestamp};
      } else {
        $self->add_nagios_critical("got no iotime from dm_os_sys_info");
      }
    } else {
      #$self->requires_version('9');
      my @monitor = $params{handle}->exec_sp_1hash(q{exec sp_monitor});
      foreach (@monitor) {
        if ($_->[0] eq 'io_busy') {
          if ($_->[1] =~ /(\d+)%/) {
            $self->{io_busy} = $1;
          }
        }
      }
      self->requires_version('9') unless defined $self->{io_busy};
    }
  } elsif ($params{mode} =~ /^server::fullscans/) {
    $self->{cnt_full_scans_s} = $self->{handle}->get_perf_counter(
        'SQLServer:Access Methods', 'Full Scans/sec');
    if (! defined $self->{cnt_full_scans_s}) {
      $self->add_nagios_unknown("unable to aquire counter data");
    } else {
      $self->valdiff(\%params, qw(cnt_full_scans_s));
      $self->{full_scans_per_sec} = $self->{delta_cnt_full_scans_s} / $self->{delta_timestamp};
    }
  } elsif ($params{mode} =~ /^server::latch::waittime/) {
    $self->{latch_wait_time} = $self->{handle}->get_perf_counter(
        "SQLServer:Latches", "Average Latch Wait Time (ms)");
    $self->{latch_wait_time_base} = $self->{handle}->get_perf_counter(
        "SQLServer:Latches", "Average Latch Wait Time Base");
    if (! defined $self->{latch_wait_time}) {
      $self->add_nagios_unknown("unable to aquire counter data");
    }
    $self->{latch_wait_time} = $self->{latch_wait_time} / $self->{latch_wait_time_base};
  } elsif ($params{mode} =~ /^server::latch::waits/) {
    $self->{latch_waits_s} = $self->{handle}->get_perf_counter(
        "SQLServer:Latches", "Latch Waits/sec");
    if (! defined $self->{latch_waits_s}) {
      $self->add_nagios_unknown("unable to aquire counter data");
    } else {
      $self->valdiff(\%params, qw(latch_waits_s));
      $self->{latch_waits_per_sec} = $self->{delta_latch_waits_s} / $self->{delta_timestamp};
    }
  } elsif ($params{mode} =~ /^server::sql::.*compilations/) {
    $self->{recompilations_s} = $self->{handle}->get_perf_counter(
        "SQLServer:SQL Statistics", "SQL Re-Compilations/sec");
    $self->{compilations_s} = $self->{handle}->get_perf_counter(
        "SQLServer:SQL Statistics", "SQL Compilations/sec");
    if (! defined $self->{recompilations_s}) {
      $self->add_nagios_unknown("unable to aquire counter data");
    } else {
      $self->valdiff(\%params, qw(recompilations_s compilations_s));
      # http://www.sqlmag.com/Articles/ArticleID/40925/pg/3/3.html
      # http://www.grumpyolddba.co.uk/monitoring/Performance%20Counter%20Guidance%20-%20SQL%20Server.htm
      $self->{delta_initial_compilations_s} = $self->{delta_compilations_s} - 
          $self->{delta_recompilations_s};
      $self->{initial_compilations_per_sec} = 
          $self->{delta_initial_compilations_s} / $self->{delta_timestamp};
      $self->{recompilations_per_sec} = 
          $self->{delta_recompilations_s} / $self->{delta_timestamp};
    }
  } elsif ($params{mode} =~ /^server::batchrequests/) {
    $self->{batch_requests_s} = $self->{handle}->get_perf_counter(
        "SQLServer:SQL Statistics", "Batch requests/sec");
    if (! defined $self->{batch_requests_s}) {
      $self->add_nagios_unknown("unable to aquire counter data");
    } else {
      $self->valdiff(\%params, qw(batch_requests_s));
      $self->{batch_requests_per_sec} = $self->{delta_batch_requests_s} / $self->{delta_timestamp};
    }
  } elsif ($params{mode} =~ /^server::totalmemory/) {
    $self->{total_memory} = $self->{handle}->get_perf_counter(
        "SQLServer:Memory Manager", "Total Server Memory (KB)");
    if (! defined $self->{total_memory}) {
      $self->add_nagios_unknown("unable to aquire counter data");
    }
  } elsif ($params{mode} =~ /^server::connectedusers/) {
    if ($self->{product} eq "ASE") {
      $self->{connectedusers} = $self->{handle}->fetchrow_array(q{
        SELECT COUNT(*) FROM master..sysprocesses WHERE hostprocess IS NOT NULL AND program_name != 'JS Agent'
      });
    } else {
      # http://www.sqlservercentral.com/articles/System+Tables/66335/
      # user processes start at 51
      $self->{connectedusers} = $self->{handle}->fetchrow_array(q{
        SELECT COUNT(*) FROM master..sysprocesses WHERE spid >= 51
      });
    }
    if (! defined $self->{connectedusers}) {
      $self->add_nagios_unknown("unable to count connected users");
    }
  } elsif ($params{mode} =~ /^server::sqlruntime/) {
    $self->set_local_db_thresholds(%params);
    my $tic = Time::HiRes::time();
      @{$self->{genericsql}} =
          $self->{handle}->fetchrow_array($params{selectname});
    $self->{runtime} = Time::HiRes::time() - $tic;
  } elsif ($params{mode} =~ /^server::sql/) {
    $self->set_local_db_thresholds(%params);
    if ($params{regexp}) {
      # sql output is treated as text
      if ($params{name2} eq $params{name}) {
        $self->add_nagios_unknown(sprintf "where's the regexp????");
      } else {
        $self->{genericsql} =
            $self->{handle}->fetchrow_array($params{selectname});
        if (! defined $self->{genericsql}) {
          $self->add_nagios_unknown(sprintf "got no valid response for %s",
              $params{selectname});
        }
      }
    } else {
      # sql output must be a number (or array of numbers)
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
  } elsif ($params{mode} =~ /^my::([^:.]+)/) {
    my $class = $1;
    my $loaderror = undef;
    substr($class, 0, 1) = uc substr($class, 0, 1);
    foreach my $libpath (split(":", $DBD::MSSQL::Server::my_modules_dyn_dir)) {
      foreach my $extmod (glob $libpath."/CheckMSSQLHealth*.pm") {
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
    if ($self->{my}->isa("DBD::MSSQL::Server")) {
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
          sprintf "Class %s is not a subclass of DBD::MSSQL::Server%s", 
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
    } elsif ($params{mode} =~ /server::database::listdatabases/) {
      foreach (sort { $a->{name} cmp $b->{name}; }  @{$self->{databases}}) {
        printf "%s\n", $_->{name};
      }
      $self->add_nagios_ok("have fun");
    } elsif ($params{mode} =~ /^server::database/) {
      foreach (@{$self->{databases}}) {
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /^server::database/) {
    } elsif ($params{mode} =~ /^server::jobs/) {
      foreach (@{$self->{jobs}}) {
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /^server::lock/) {
      foreach (@{$self->{locks}}) {
        $_->nagios(%params);
        $self->merge_nagios($_);
      }
    } elsif ($params{mode} =~ /^server::memorypool/) {
      $self->{memorypool}->nagios(%params);
      $self->merge_nagios($self->{memorypool});
    } elsif ($params{mode} =~ /^server::connectiontime/) {
      $self->add_nagios(
          $self->check_thresholds($self->{connection_time}, 1, 5),
          sprintf "%.2f seconds to connect as %s",
              $self->{connection_time}, $self->{dbuser});
      $self->add_perfdata(sprintf "connection_time=%.2f;%d;%d",
          $self->{connection_time},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::cpubusy/) {
      $self->add_nagios(
          $self->check_thresholds($self->{cpu_busy}, 80, 90),
          sprintf "CPU busy %.2f%%", $self->{cpu_busy});
      $self->add_perfdata(sprintf "cpu_busy=%.2f;%s;%s",
          $self->{cpu_busy},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::iobusy/) {
      $self->add_nagios(
          $self->check_thresholds($self->{io_busy}, 80, 90),
          sprintf "IO busy %.2f%%", $self->{io_busy});
      $self->add_perfdata(sprintf "io_busy=%.2f;%s;%s",
          $self->{io_busy},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::fullscans/) {
      $self->add_nagios(
          $self->check_thresholds($self->{full_scans_per_sec}, 100, 500),
          sprintf "%.2f full table scans / sec", $self->{full_scans_per_sec});
      $self->add_perfdata(sprintf "full_scans_per_sec=%.2f;%s;%s",
          $self->{full_scans_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::latch::waits/) {
      $self->add_nagios(
          $self->check_thresholds($self->{latch_waits_per_sec}, 10, 50),
          sprintf "%.2f latches / sec have to wait", $self->{latch_waits_per_sec});
      $self->add_perfdata(sprintf "latch_waits_per_sec=%.2f;%s;%s",
          $self->{latch_waits_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::latch::waittime/) {
      $self->add_nagios(
          $self->check_thresholds($self->{latch_wait_time}, 1, 5),
          sprintf "latches have to wait %.2f ms avg", $self->{latch_wait_time});
      $self->add_perfdata(sprintf "latch_avg_wait_time=%.2fms;%s;%s",
          $self->{latch_wait_time},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::sql::recompilations/) {
      $self->add_nagios(
          $self->check_thresholds($self->{recompilations_per_sec}, 1, 10),
          sprintf "%.2f SQL recompilations / sec", $self->{recompilations_per_sec});
      $self->add_perfdata(sprintf "sql_recompilations_per_sec=%.2f;%s;%s",
          $self->{recompilations_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::sql::initcompilations/) {
      $self->add_nagios(
          $self->check_thresholds($self->{initial_compilations_per_sec}, 100, 200),
          sprintf "%.2f initial compilations / sec", $self->{initial_compilations_per_sec});
      $self->add_perfdata(sprintf "sql_initcompilations_per_sec=%.2f;%s;%s",
          $self->{initial_compilations_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::batchrequests/) {
      $self->add_nagios(
          $self->check_thresholds($self->{batch_requests_per_sec}, 100, 200),
          sprintf "%.2f batch requests / sec", $self->{batch_requests_per_sec});
      $self->add_perfdata(sprintf "batch_requests_per_sec=%.2f;%s;%s",
          $self->{batch_requests_per_sec},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::totalmemory/) {
      $self->add_nagios(
          $self->check_thresholds($self->{total_memory}, 1000, 5000),
          sprintf "total server memory %ld", $self->{total_memory});
      $self->add_perfdata(sprintf "total_server_memory=%ld;%s;%s",
          $self->{total_memory},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::connectedusers/) {
      $self->add_nagios(
          $self->check_thresholds($self->{connectedusers}, 50, 80),
          sprintf "%d connected users", $self->{connectedusers});
      $self->add_perfdata(sprintf "connected_users=%d;%s;%s",
          $self->{connectedusers},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::sqlruntime/) {
      $self->add_nagios(
          $self->check_thresholds($self->{runtime}, 1, 5),
          sprintf "%.2f seconds to execute %s",
              $self->{runtime},
              $params{name2} ? $params{name2} : $params{selectname});
      $self->add_perfdata(sprintf "sql_runtime=%.4f;%d;%d",
          $self->{runtime},
          $self->{warningrange}, $self->{criticalrange});
    } elsif ($params{mode} =~ /^server::sql/) {
      if ($params{regexp}) {
        if (substr($params{name2}, 0, 1) eq '!') {
          $params{name2} =~ s/^!//;
          if ($self->{genericsql} !~ /$params{name2}/) {
            $self->add_nagios_ok(
                sprintf "output %s does not match pattern %s",
                    $self->{genericsql}, $params{name2}); 
          } else {
            $self->add_nagios_critical(
                sprintf "output %s matches pattern %s",
                    $self->{genericsql}, $params{name2});
          }
        } else {
          if ($self->{genericsql} =~ /$params{name2}/) {
            $self->add_nagios_ok(
                sprintf "output %s matches pattern %s",
                    $self->{genericsql}, $params{name2});
          } else {
            $self->add_nagios_critical(
                sprintf "output %s does not match pattern %s",
                    $self->{genericsql}, $params{name2});
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
  if ($self->{warningrange} =~ /^(\d+)$/) {
    # warning = 10, warn if > 10 or < 0
    $level = $ERRORS{WARNING}
        if ($value > $1 || $value < 0);
  } elsif ($self->{warningrange} =~ /^(\d+):$/) {
    # warning = 10:, warn if < 10
    $level = $ERRORS{WARNING}
        if ($value < $1);
  } elsif ($self->{warningrange} =~ /^~:(\d+)$/) {
    # warning = ~:10, warn if > 10
    $level = $ERRORS{WARNING}
        if ($value > $1);
  } elsif ($self->{warningrange} =~ /^(\d+):(\d+)$/) {
    # warning = 10:20, warn if < 10 or > 20
    $level = $ERRORS{WARNING}
        if ($value < $1 || $value > $2);
  } elsif ($self->{warningrange} =~ /^@(\d+):(\d+)$/) {
    # warning = @10:20, warn if >= 10 and <= 20
    $level = $ERRORS{WARNING}
        if ($value >= $1 && $value <= $2);
  }
  if ($self->{criticalrange} =~ /^(\d+)$/) {
    # critical = 10, crit if > 10 or < 0
    $level = $ERRORS{CRITICAL}
        if ($value > $1 || $value < 0);
  } elsif ($self->{criticalrange} =~ /^(\d+):$/) {
    # critical = 10:, crit if < 10
    $level = $ERRORS{CRITICAL}
        if ($value < $1);
  } elsif ($self->{criticalrange} =~ /^~:(\d+)$/) {
    # critical = ~:10, crit if > 10
    $level = $ERRORS{CRITICAL}
        if ($value > $1);
  } elsif ($self->{criticalrange} =~ /^(\d+):(\d+)$/) {
    # critical = 10:20, crit if < 10 or > 20
    $level = $ERRORS{CRITICAL}
        if ($value < $1 || $value > $2);
  } elsif ($self->{criticalrange} =~ /^@(\d+):(\d+)$/) {
    # critical = @10:20, crit if >= 10 and <= 20
    $level = $ERRORS{CRITICAL}
        if ($value >= $1 && $value <= $2);
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
  foreach my $llevel (qw(CRITICAL WARNING UNKNOWN OK)) {
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
    my $find_sql = undef;
    if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
      $find_sql = q{
          SELECT name FROM sys.objects
          WHERE name = 'check_mssql_health_thresholds'
      };
    } else {
      $find_sql = q{
          SELECT name FROM sysobjects
          WHERE name = 'check_mssql_health_thresholds'
      };
    }
    if ($self->{handle}->fetchrow_array($find_sql)) {
      my @dbthresholds = $self->{handle}->fetchall_array(q{
          SELECT * FROM check_mssql_health_thresholds
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
  if ($DBD::MSSQL::Server::verbose) {
    printf "%s %s\n", $msg, ref($self);
  }
}

sub dbconnect {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  $self->{tic} = Time::HiRes::time();
  $self->{handle} = DBD::MSSQL::Server::Connection->new(%params);
  if ($self->{handle}->{errstr}) {
    if ($self->{handle}->{errstr} eq "alarm\n") {
      $self->add_nagios($ERRORS{CRITICAL},
          sprintf "connection could not be established within %d seconds",
              $self->{timeout});
    } else {
      $self->add_nagios($ERRORS{CRITICAL},
          sprintf "cannot connect to %s. %s",
          ($self->{server} ? $self->{server} :
          ($self->{hostname} ? $self->{hostname} : "unknown host")),
          $self->{handle}->{errstr});
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
  if (! @_) {
    # falls im sql-statement % vorkommen. sonst krachts im printf
    $format =~ s/%/%%/g;
  }
  $self->{trace} = -f "/tmp/check_mssql_health.trace" ? 1 : 0;
  if ($DBD::MSSQL::Server::verbose) {
    printf("%s: ", scalar localtime);
    printf($format, @_);
  }
  if ($self->{trace}) {
    my $logfh = new IO::File;
    $logfh->autoflush(1);
    if ($logfh->open("/tmp/check_mssql_health.trace", "a")) {
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
  #$self->trace(sprintf "DESTROY %s with handle %s %s", ref($self), $handle1, $handle2);
  if (ref($self) eq "DBD::MSSQL::Server") {
  }
  #$self->trace(sprintf "DESTROY %s exit with handle %s %s", ref($self), $handle1, $handle2);
  if (ref($self) eq "DBD::MSSQL::Server") {
    #printf "humpftata\n";
  }
}

sub save_state {
  my $self = shift;
  my %params = @_;
  my $extension = "";
  my $mode = $params{mode};
  if ($params{connect} && $params{connect} =~ /(\w+)\/(\w+)@(\w+)/) {
    $params{connect} = $3;
  } elsif ($params{connect}) {
    # just to be sure
    $params{connect} =~ s/\//_/g;
  }
  if ($^O =~ /MSWin/) {
    $mode =~ s/::/_/g;
    $params{statefilesdir} = $self->system_vartmpdir();
  }
  if (! -d $params{statefilesdir}) {
    eval {
      use File::Path;
      mkpath $params{statefilesdir};
    };
  }
  if ($@ || ! -w $params{statefilesdir}) {
    $self->add_nagios($ERRORS{CRITICAL},
        sprintf "statefilesdir %s does not exist or is not writable\n",
        $params{statefilesdir});
    return;
  }
  my $statefile = sprintf "%s_%s", ($params{hostname} || $params{server}), $mode;
  $extension .= $params{differenciator} ? "_".$params{differenciator} : "";
  $extension .= $params{port} ? "_".$params{port} : "";
  $extension .= $params{database} ? "_".$params{database} : "";
  $extension .= $params{name} ? "_".$params{name} : "";
  $extension =~ s/\//_/g;
  $extension =~ s/\(/_/g;
  $extension =~ s/\)/_/g;
  $extension =~ s/\*/_/g;
  $extension =~ s/\s/_/g;
  $statefile .= $extension;
  $statefile = lc $statefile;
  $statefile = sprintf "%s/%s", $params{statefilesdir}, $statefile;
  if (open(STATE, ">$statefile")) {
    if ((ref($params{save}) eq "HASH") && exists $params{save}->{timestamp}) {
      $params{save}->{localtime} = scalar localtime $params{save}->{timestamp};
    }
    printf STATE Data::Dumper::Dumper($params{save});
    close STATE;
  } else {
    $self->add_nagios($ERRORS{CRITICAL},
        sprintf "statefile %s is not writable", $statefile);
  }
  $self->debug(sprintf "saved %s to %s",
      Data::Dumper::Dumper($params{save}), $statefile);
}

sub load_state {
  my $self = shift;
  my %params = @_;
  my $extension = "";
  my $mode = $params{mode};
  if ($params{connect} && $params{connect} =~ /(\w+)\/(\w+)@(\w+)/) {
    $params{connect} = $3;
  } elsif ($params{connect}) {
    # just to be sure
    $params{connect} =~ s/\//_/g;
  }
  if ($^O =~ /MSWin/) {
    $mode =~ s/::/_/g;
    $params{statefilesdir} = $self->system_vartmpdir();
  }
  my $statefile = sprintf "%s_%s", ($params{hostname} || $params{server}), $mode;
  $extension .= $params{differenciator} ? "_".$params{differenciator} : "";
  $extension .= $params{port} ? "_".$params{port} : "";
  $extension .= $params{database} ? "_".$params{database} : "";
  $extension .= $params{name} ? "_".$params{name} : "";
  $extension =~ s/\//_/g;
  $extension =~ s/\(/_/g;
  $extension =~ s/\)/_/g;
  $extension =~ s/\*/_/g;
  $extension =~ s/\s/_/g;
  $statefile .= $extension;
  $statefile = lc $statefile;
  $statefile = sprintf "%s/%s", $params{statefilesdir}, $statefile;
  if ( -f $statefile) {
    our $VAR1;
    eval {
      require $statefile;
    };
    if($@) {
      $self->add_nagios($ERRORS{CRITICAL},
          sprintf "statefile %s is corrupt", $statefile);
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
  my $last_values = $self->load_state(%params) || eval {
    my $empty_events = {};
    foreach (@keys) {
      $empty_events->{$_} = 0;
    }
    $empty_events->{timestamp} = 0;
    $empty_events;
  };
  foreach (@keys) {
    $last_values->{$_} = 0 if ! exists $last_values->{$_};
    if ($self->{$_} >= $last_values->{$_}) {
      $self->{'delta_'.$_} = $self->{$_} - $last_values->{$_};
    } else {
      # vermutlich db restart und zaehler alle auf null
      $self->{'delta_'.$_} = $self->{$_};
    }
    $self->debug(sprintf "delta_%s %f", $_, $self->{'delta_'.$_});
  }
  $self->{'delta_timestamp'} = time - $last_values->{timestamp};
  $params{save} = eval {
    my $empty_events = {};
    foreach (@keys) {
      $empty_events->{$_} = $self->{$_};
    }
    $empty_events->{timestamp} = time;
    $empty_events;
  };
  $self->save_state(%params);
}

sub requires_version {
  my $self = shift;
  my $version = shift;
  my @instances = DBD::MSSQL::Server::return_servers();
  my $instversion = $instances[0]->{version};
  if (! $self->version_is_minimum($version)) {
    $self->add_nagios($ERRORS{UNKNOWN}, 
        sprintf "not implemented/possible for MSSQL release %s", $instversion);
  }
}

sub version_is_minimum {
  # the current version is newer or equal
  my $self = shift;
  my $version = shift;
  my $newer = 1;
  my @instances = DBD::MSSQL::Server::return_servers();
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
  my @instances = DBD::MSSQL::Server::return_servers();
  return (lc $instances[0]->{parallel} eq "yes") ? 1 : 0;
}

sub instance_thread {
  my $self = shift;
  my @instances = DBD::MSSQL::Server::return_servers();
  return $instances[0]->{thread};
}

sub windows_server {
  my $self = shift;
  my @instances = DBD::MSSQL::Server::return_servers();
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
    return "/var/tmp/check_mssql_health";
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


package DBD::MSSQL::Server::Connection;

use strict;

our @ISA = qw(DBD::MSSQL::Server);


sub new {
  my $class = shift;
  my %params = @_;
  my $self = {
    mode => $params{mode},
    timeout => $params{timeout},
    method => $params{method} || "dbi",
    hostname => $params{hostname},
    username => $params{username},
    password => $params{password},
    verbose => $params{verbose},
    port => $params{port} || 1433,
    server => $params{server},
    currentdb => $params{currentdb},
    handle => undef,
  };
  bless $self, $class;
  if ($params{method} eq "dbi") {
    bless $self, "DBD::MSSQL::Server::Connection::Dbi";
  } elsif ($params{method} eq "sqsh") {
    bless $self, "DBD::MSSQL::Server::Connection::Sqsh";
  } elsif ($params{method} eq "sqlrelay") {
    bless $self, "DBD::MSSQL::Server::Connection::Sqlrelay";
  } elsif ($params{method} eq "sqlcmd") {
    bless $self, "DBD::MSSQL::Server::Connection::Sqlcmd";
  }
  $self->init(%params);
  return $self;
}

sub get_instance_names {
  my $self = shift;
  my $object_name = shift;
  my $servicename = DBD::MSSQL::Server::return_first_server()->{servicename};
  if ($object_name =~ /SQLServer:(.*)/) {
    $object_name = $servicename.':'.$1;
  }
  if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
    return $self->fetchall_array(q{
        SELECT
            DISTINCT instance_name
        FROM
            sys.dm_os_performance_counters
        WHERE
            object_name = ?
    }, $object_name);
  } else {
    return $self->fetchall_array(q{
        SELECT
            DISTINCT instance_name
        FROM
            master.dbo.sysperfinfo
        WHERE
            object_name = ?
    }, $object_name);
  }
}

sub get_perf_counter {
  my $self = shift;
  my $object_name = shift;
  my $counter_name = shift;
  my $servicename = DBD::MSSQL::Server::return_first_server()->{servicename};
  if ($object_name =~ /SQLServer:(.*)/) {
    $object_name = $servicename.':'.$1;
  }
  if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
    return $self->fetchrow_array(q{
        SELECT
            cntr_value
        FROM
            sys.dm_os_performance_counters
        WHERE
            counter_name = ? AND
            object_name = ?
    }, $counter_name, $object_name);
  } else {
    return $self->fetchrow_array(q{
        SELECT
            cntr_value
        FROM
            master.dbo.sysperfinfo
        WHERE
            counter_name = ? AND
            object_name = ?
    }, $counter_name, $object_name);
  }
}

sub get_perf_counter_instance {
  my $self = shift;
  my $object_name = shift;
  my $counter_name = shift;
  my $instance_name = shift;
  my $servicename = DBD::MSSQL::Server::return_first_server()->{servicename};
  if ($object_name =~ /SQLServer:(.*)/) {
    $object_name = $servicename.':'.$1;
  }
  if (DBD::MSSQL::Server::return_first_server()->version_is_minimum("9.x")) {
    return $self->fetchrow_array(q{
        SELECT
            cntr_value
        FROM
            sys.dm_os_performance_counters
        WHERE
            counter_name = ? AND
            object_name = ? AND
            instance_name = ?
    }, $counter_name, $object_name, $instance_name);
  } else {
    return $self->fetchrow_array(q{
        SELECT
            cntr_value
        FROM
            master.dbo.sysperfinfo
        WHERE
            counter_name = ? AND
            object_name = ? AND
            instance_name = ?
    }, $counter_name, $object_name, $instance_name);
  }
}

package DBD::MSSQL::Server::Connection::Dbi;

use strict;
use Net::Ping;
use File::Basename;

our @ISA = qw(DBD::MSSQL::Server::Connection);


sub init {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  if ($self->{mode} =~ /^server::tnsping/) {
    # erstmal reserviert fuer irgendeinen tcp-connect
    if (! $self->{connect}) {
      $self->{errstr} = "Please specify a database";
    } else {
      $self->{sid} = $self->{connect};
      $self->{username} ||= time;  # prefer an existing user
      $self->{password} = time;
    }
  } else {
    if ((! $self->{hostname} && ! $self->{server}) ||
        ! $self->{username} || ! $self->{password}) {
      $self->{errstr} = "Please specify hostname or server, username and password";
      return undef;
    }
    $self->{dbi_options} = { RaiseError => 1, AutoCommit => 0, PrintError => 1 };
    $self->{dsn} = "DBI:Sybase:";
    if ($self->{hostname}) {
      $self->{dsn} .= sprintf ";host=%s", $self->{hostname};
      $self->{dsn} .= sprintf ";port=%s", $self->{port};
    } else {
      $self->{dsn} .= sprintf ";server=%s", $self->{server};
    }
    if ($params{currentdb}) {
      if (index($params{currentdb},"-") != -1) {
        $self->{dsn} .= sprintf ";database=\"%s\"", $params{currentdb};
      } else {
        $self->{dsn} .= sprintf ";database=%s", $params{currentdb};
      }
    }
    if (basename($0) =~ /_sybase_/) {
      $self->{dbi_options}->{syb_chained_txn} = 1;
      $self->{dsn} .= sprintf ";tdsLevel=CS_TDS_42";
    }
  }
  if (! exists $self->{errstr}) {
    my $stderrvar;
    eval {
      require DBI;
      use POSIX ':signal_h';
      if ($^O =~ /MSWin/) {
        local $SIG{'ALRM'} = sub {
          die "alarm\n";
        };
      } else {
        my $mask = POSIX::SigSet->new( SIGALRM );
        my $action = POSIX::SigAction->new(
            sub { die "alarm\n" ; }, $mask);
        my $oldaction = POSIX::SigAction->new();
        sigaction(SIGALRM ,$action ,$oldaction );
      }
      alarm($self->{timeout} - 1); # 1 second before the global unknown timeout
      *SAVEERR = *STDERR;
      open OUT ,'>',\$stderrvar;
      *STDERR = *OUT;
      if ($self->{handle} = DBI->connect(
          $self->{dsn},
          $self->{username},
          $self->{password},
          $self->{dbi_options})) {
        $retval = $self;
      } else {
        # doesnt seem to work $self->{errstr} = DBI::errstr();
        $self->{errstr} = "connect failed";
        return undef;
      }
      *STDERR = *SAVEERR;
    };
    if ($@) {
      $self->{errstr} = $@;
      $retval = undef;
    } elsif ($stderrvar && $stderrvar =~ /can't change context to database/) {
      $self->{errstr} = $stderrvar;
    } else {
      $self->{errstr} = "";
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
  my $stderrvar;
  *SAVEERR = *STDERR;
  open ERR ,'>',\$stderrvar;
  *STDERR = *ERR;
  eval {
    if ($self->{dsn} =~ /tdsLevel/) {
      # better install a handler here. otherwise the plugin output is
      # unreadable when errors occur
      $self->{handle}->{syb_err_handler} = sub {
        my($err, $sev, $state, $line, $server,
            $proc, $msg, $sql, $err_type) = @_;
        #push(@errrow, $msg);
        $self->{errstr} = join("\n", (split(/\n/, $self->{errstr}), $msg));
        return 0;
      };
    }
    $self->trace(sprintf "SQL:\n%s\nARGS:\n%s\n",
        $sql, Data::Dumper::Dumper(\@arguments));
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments) || die DBI::errstr();
    } else {
      $sth->execute() || die DBI::errstr();
    }
    if (lc $sql =~ /^\s*(exec |sp_)/ || $sql =~ /^\s*exec sp/im) {
      # flatten the result sets
      do {
        while (my $aref = $sth->fetchrow_arrayref()) {
          push(@row, @{$aref});
        }
      } while ($sth->{syb_more_results});
    } else {
      @row = $sth->fetchrow_array();
    }
    $self->trace(sprintf "RESULT:\n%s\n",
        Data::Dumper::Dumper(\@row));
  }; 
  *STDERR = *SAVEERR;
  $self->{errstr} = join("\n", (split(/\n/, $self->{errstr}), $stderrvar)) if $stderrvar;
  if ($@) {
    $self->trace(sprintf "bumm %s", $@);
  }
  if ($stderrvar) {
    $self->trace(sprintf "stderr %s", $self->{errstr}) ;
  }
  if (-f "/tmp/check_mssql_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) = 
        "/tmp/check_mssql_health_simulation/".$self->{mode}; <> };
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
  my $stderrvar;
  *SAVEERR = *STDERR;
  open ERR ,'>',\$stderrvar;
  *STDERR = *ERR;
  eval {
    $self->trace(sprintf "SQL:\n%s\nARGS:\n%s\n",
        $sql, Data::Dumper::Dumper(\@arguments));
    if ($sql =~ /^\s*dbcc /im) {
      # dbcc schreibt auf stdout. Die Ausgabe muss daher
      # mit einem eigenen Handler aufgefangen werden.
      $self->{handle}->{syb_err_handler} = sub {
        my($err, $sev, $state, $line, $server,
            $proc, $msg, $sql, $err_type) = @_;
        push(@{$rows}, $msg);
        return 0;
      };
    }
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    if ($sql !~ /^\s*dbcc /im) {
      $rows = $sth->fetchall_arrayref();
    }
    $self->trace(sprintf "RESULT:\n%s\n",
        Data::Dumper::Dumper($rows));
  }; 
  *STDERR = *SAVEERR;
  $self->{errstr} = join("\n", (split(/\n/, $self->{errstr}), $stderrvar)) if $stderrvar;
  if ($@) {
    $self->trace(sprintf "bumm %s", $@);
    $rows = [];
  }
  if ($stderrvar) {
    $self->trace(sprintf "stderr %s", $self->{errstr}) ;
  }
  if (-f "/tmp/check_mssql_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) = 
        "/tmp/check_mssql_health_simulation/".$self->{mode}; <> };
    @{$rows} = map { [ split(/\s+/, $_) ] } split(/\n/, $simulation);
  }
  return @{$rows};
}

sub exec_sp_1hash {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my $rows = undef;
  eval {
    $self->trace(sprintf "SQL:\n%s\nARGS:\n%s\n",
        $sql, Data::Dumper::Dumper(\@arguments));
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    do {
      while (my $href = $sth->fetchrow_hashref()) {
        foreach (keys %{$href}) {
          push(@{$rows}, [ $_, $href->{$_} ]);
        }
      }
    } while ($sth->{syb_more_results});
    $self->trace(sprintf "RESULT:\n%s\n",
        Data::Dumper::Dumper($rows));
  };
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
  return @{$rows};
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

package DBD::MSSQL::Server::Connection::Sqlcmd;

use strict;
use File::Temp qw/tempfile/;

our @ISA = qw(DBD::MSSQL::Server::Connection);


sub init {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  $self->{loginstring} = "hostport";
  my $template = $self->{mode}.'XXXXX';
  if ($^O =~ /MSWin/) {
    $template =~ s/::/_/g;
  }
  ($self->{sql_commandfile_handle}, $self->{sql_commandfile}) =
      tempfile($template, SUFFIX => ".sql",
      DIR => $self->system_tmpdir() );
  close $self->{sql_commandfile_handle};
  ($self->{sql_resultfile_handle}, $self->{sql_resultfile}) =
      tempfile($template, SUFFIX => ".out",
      DIR => $self->system_tmpdir() );
  close $self->{sql_resultfile_handle};
  ($self->{sql_outfile_handle}, $self->{sql_outfile}) =
      tempfile($template, SUFFIX => ".out",
      DIR => $self->system_tmpdir() );
  close $self->{sql_outfile_handle};

  if ($self->{mode} =~ /^server::tnsping/) {
    die "oracle leftover";
  } else {
    # --server xy --username xy --password xy
    # --hostname xy --username xy --password xy
    # --hostname xy --port --username xy --password xy
    if ($self->{server} && $self->{username} && $self->{password}) {
      # --server bba --user nagios --password oradbmon
      $self->{loginstring} = "server";
    } elsif ($self->{hostname} && $self->{username} && $self->{password}) {
      # --hostname bba --user nagios --password oradbmon
      $self->{loginstring} = "server";
      $self->{server} = sprintf 'tcp:%s,%s', $self->{hostname}, $self->{port};
    } else {
      $self->{errstr} = "Please specify servername, username and password";
      return undef;
    }
  }
  if (! exists $self->{errstr}) {
    eval {
      if (! exists $ENV{SQL_HOME}) {
        foreach my $path (split(';', $ENV{PATH})) {
          $self->trace(sprintf "try to find sqlcmd.exe in %s", $path);
          if (-x $path.'/sqlcmd.exe') {
            $ENV{SQL_HOME} = $path;
            last;
          }
        }
        $ENV{SQL_HOME} |= '';
      } else {
        $ENV{PATH} = $ENV{SQL_HOME}.
            (defined $ENV{PATH} ? ";".$ENV{PATH} : "");
      }
      my $sqlcmd = undef;
      if (-x $ENV{SQL_HOME}.'/'.'sqlcmd.exe') {
        $sqlcmd = $ENV{SQL_HOME}.'/'.'sqlcmd.exe';
      }
      if (! $sqlcmd) {
        die "nosqlcmd\n";
      } else {
        $self->trace(sprintf "found %s", $sqlcmd);
      }
      if ($self->{mode} =~ /^server::tnsping/) {
        die "oracle leftover";
      } else {
        if ($self->{loginstring} eq "server") {
          $self->{sqlcmd} = sprintf '"%s" -S %s -U "%s" -P "%s" %s -i "%s" -o "%s"',
              $sqlcmd, $self->{server}, $self->{username}, $self->{password},
              ($self->{currentdb} ? "-d ".$self->{currentdb} : ""),
              $self->{sql_commandfile}, $self->{sql_resultfile};
          $self->{sqlcmd} .= ' -h-1 -s"|" -W';
        }
      }
  
      use POSIX ':signal_h';
      if ($^O =~ /MSWin/) {
        local $SIG{'ALRM'} = sub {
          die "alarm\n";
        };
      } else {
        my $mask = POSIX::SigSet->new( SIGALRM );
        my $action = POSIX::SigAction->new(
            sub { die "alarm\n" ; }, $mask);
        my $oldaction = POSIX::SigAction->new();
        sigaction(SIGALRM ,$action ,$oldaction );
      }
      alarm($self->{timeout} - 1); # 1 second before the global unknown timeout

      my $answer = $self->fetchrow_array(
          q{ SELECT 'schnorch' });
      die unless defined $answer and $answer eq 'schnorch';
      $retval = $self;
    };
    if ($@) {
      $self->{errstr} = $@;
      $self->{errstr} =~ s/at $0 .*//g;
      chomp $self->{errstr};
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
  foreach (@arguments) {
    # replace the ? by the parameters
    if (/^\d+$/) {
      $sql =~ s/\?/$_/;
    } else {
      $sql =~ s/\?/'$_'/;
    }
  }
  $self->trace(sprintf "SQL (? resolved):\n%s\nARGS:\n%s\n",
      $sql, Data::Dumper::Dumper(\@arguments));
  $self->create_commandfile($sql);
  my $exit_output = `$self->{sqlcmd}`;
  if ($?) {
    printf STDERR "fetchrow_array exit bumm \n";
    my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; <> };
    my @oerrs = map {
      /(ORA\-\d+:.*)/ ? $1 : ();
    } split(/\n/, $output);
    $self->{errstr} = join(" ", @oerrs);
  } else {
    my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; <> };
    @row = map { convert($_) } 
        map { s/^\s+([\.\d]+)$/$1/g; $_ }         # strip leading space from numbers
        map { s/\s+$//g; $_ }                     # strip trailing space
        split(/\|/, (split(/\n/, $output))[0]);
    $self->trace(sprintf "RESULT:\n%s\n",
        Data::Dumper::Dumper(\@row));
  }
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
  }
  unlink $self->{sql_commandfile};
  unlink $self->{sql_resultfile};
  return $row[0] unless wantarray;
  return @row;
}

sub fetchall_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my $rows = undef;
  foreach (@arguments) {
    # replace the ? by the parameters
    if (/^\d+$/) {
      $sql =~ s/\?/$_/;
    } else {
      $sql =~ s/\?/'$_'/;
    }
  }
  $self->trace(sprintf "SQL (? resolved):\n%s\nARGS:\n%s\n",
      $sql, Data::Dumper::Dumper(\@arguments));
  $self->create_commandfile($sql);
  my $exit_output = `$self->{sqlcmd}`;
  if ($?) {
    printf STDERR "fetchrow_array exit bumm %s\n", $exit_output;
    my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; <> };
    my @oerrs = map {
      /(ORA\-\d+:.*)/ ? $1 : ();
    } split(/\n/, $output);
    $self->{errstr} = join(" ", @oerrs);
  } else {
    my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; <> };
    my @rows = map { [ 
        map { convert($_) } 
        map { s/^\s+([\.\d]+)$/$1/g; $_ }
        map { s/\s+$//g; $_ }
        split /\|/
    ] } grep { ! /^\(\d+ rows affected\)/ } 
        grep { ! /^\s*$/ }
        grep { ! /^Database name .* ignored, referencing object in/ } split(/\n/, $output);
    $rows = \@rows;
    $self->trace(sprintf "RESULT:\n%s\n",
        Data::Dumper::Dumper($rows));
  }
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
  }
  unlink $self->{sql_commandfile};
  unlink $self->{sql_resultfile};
  return @{$rows};
}

sub func {
  my $self = shift;
  my $function = shift;
  $self->{handle}->func(@_);
}

sub convert {
  my $n = shift;
  # mostly used to convert numbers in scientific notation
  if ($n =~ /^\s*\d+\s*$/) {
    return $n;
  } elsif ($n =~ /^\s*([-+]?)(\d*[\.,]*\d*)[eE]{1}([-+]?)(\d+)\s*$/) {
    my ($vor, $num, $sign, $exp) = ($1, $2, $3, $4);
    $n =~ s/E/e/g;
    $n =~ s/,/\./g;
    $num =~ s/,/\./g;
    my $sig = $sign eq '-' ? "." . ($exp - 1 + length $num) : '';
    my $dec = sprintf "%${sig}f", $n;
    $dec =~ s/\.[0]+$//g;
    return $dec;
  } elsif ($n =~ /^\s*([-+]?)(\d+)[\.,]*(\d*)\s*$/) {
    return $1.$2.".".$3;
  } elsif ($n =~ /^\s*(.*?)\s*$/) {
    return $1;
  } else {
    return $n;
  }
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
  $self->trace("try to clean up command and result files");
  unlink $self->{sql_commandfile} if -f $self->{sql_commandfile};
  unlink $self->{sql_resultfile} if -f $self->{sql_resultfile};
}

sub create_commandfile {
  my $self = shift;
  my $sql = shift;
  open CMDCMD, "> $self->{sql_commandfile}";
  printf CMDCMD "%s\n", $sql;
  printf CMDCMD "go\n";
  close CMDCMD;
}

package DBD::MSSQL::Server::Connection::Sqsh;

use strict;
use File::Temp qw/tempfile/;

our @ISA = qw(DBD::MSSQL::Server::Connection);


sub init {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  $self->{loginstring} = "hostport";
  my $template = $self->{mode}.'XXXXX';
  if ($^O =~ /MSWin/) {
    $template =~ s/::/_/g;
  }
  ($self->{sql_commandfile_handle}, $self->{sql_commandfile}) =
      tempfile($template, SUFFIX => ".sql",
      DIR => $self->system_tmpdir() );
  close $self->{sql_commandfile_handle};
  ($self->{sql_resultfile_handle}, $self->{sql_resultfile}) =
      tempfile($template, SUFFIX => ".out",
      DIR => $self->system_tmpdir() );
  close $self->{sql_resultfile_handle};
  ($self->{sql_outfile_handle}, $self->{sql_outfile}) =
      tempfile($template, SUFFIX => ".out",
      DIR => $self->system_tmpdir() );
  close $self->{sql_outfile_handle};

  if ($self->{mode} =~ /^server::tnsping/) {
    die "oracle leftover";
  } else {
    # --server xy --username xy --password xy
    # --hostname xy --username xy --password xy
    # --hostname xy --port --username xy --password xy
    if ($self->{server} && $self->{username} && $self->{password}) {
      # --server bba --user nagios --password oradbmon
      $self->{loginstring} = "server";
    } elsif ($self->{hostname} && $self->{username} && $self->{password}) {
      # --hostname bba --user nagios --password oradbmon
      $self->{loginstring} = "server";
      $self->{server} = sprintf 'tcp:%s,%s', $self->{hostname}, $self->{port};
    } else {
      $self->{errstr} = "Please specify servername, username and password";
      return undef;
    }
  }
  if (! exists $self->{errstr}) {
    eval {
      if (! exists $ENV{SQL_HOME}) {
        if ($^O =~ /MSWin/) {
          foreach my $path (split(';', $ENV{PATH})) {
            if (-x $path.'/sqsh.exe') {
              $ENV{SQL_HOME} = $path;
              last;
            }
          }
        } else {
          foreach my $path (split(':', $ENV{PATH})) {
            if (-x $path.'/bin/sqsh') {
              $ENV{SQL_HOME} = $path;
              last;
            }
          }
        }
        $ENV{SQL_HOME} |= '';
      } else {
        if ($^O =~ /MSWin/) {
          $ENV{PATH} = $ENV{SQL_HOME}.
              (defined $ENV{PATH} ? ";".$ENV{PATH} : "");
        } else {
          $ENV{PATH} = $ENV{SQL_HOME}."/bin".
              (defined $ENV{PATH} ? ":".$ENV{PATH} : "");
          $ENV{LD_LIBRARY_PATH} = $ENV{SQL_HOME}."/lib".
              (defined $ENV{LD_LIBRARY_PATH} ? ":".$ENV{LD_LIBRARY_PATH} : "");
        }
      }
      my $sqsh = undef;
      my $tnsping = undef;
      if (-x $ENV{SQL_HOME}.'/'.'bin'.'/'.'sqsh') {
        $sqsh = $ENV{SQL_HOME}.'/'.'bin'.'/'.'sqsh';
      } elsif (-x $ENV{SQL_HOME}.'/'.'sqsh') {
        $sqsh = $ENV{SQL_HOME}.'/'.'sqsh';
      } elsif (-x $ENV{SQL_HOME}.'/'.'sqsh.exe') {
        $sqsh = $ENV{SQL_HOME}.'/'.'sqsh.exe';
      } elsif (-x '/usr/bin/sqsh') {
        $sqsh = '/usr/bin/sqsh';
      }
      if (! $sqsh) {
        die "nosqsh\n";
      }
      if ($self->{mode} =~ /^server::tnsping/) {
        die "oracle leftover";
      } else {
        if ($self->{loginstring} eq "server") {
          $self->{sqsh} = sprintf '"%s" -S %s -U "%s" -P "%s" -i "%s" -o "%s"',
              $sqsh, $self->{server}, $self->{username}, $self->{password},
              $self->{sql_commandfile}, $self->{sql_resultfile};
          $self->{sqsh} .= ' -h -s"|"';
        }
      }
  
      use POSIX ':signal_h';
      if ($^O =~ /MSWin/) {
        local $SIG{'ALRM'} = sub {
          die "alarm\n";
        };
      } else {
        my $mask = POSIX::SigSet->new( SIGALRM );
        my $action = POSIX::SigAction->new(
            sub { die "alarm\n" ; }, $mask);
        my $oldaction = POSIX::SigAction->new();
        sigaction(SIGALRM ,$action ,$oldaction );
      }
      alarm($self->{timeout} - 1); # 1 second before the global unknown timeout

      my $answer = $self->fetchrow_array(
          q{ SELECT 'schnorch' });
      die unless defined $answer and $answer eq 'schnorch';
      $retval = $self;
    };
    if ($@) {
      $self->{errstr} = $@;
      $self->{errstr} =~ s/at $0 .*//g;
      chomp $self->{errstr};
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
  foreach (@arguments) {
    # replace the ? by the parameters
    if (/^\d+$/) {
      $sql =~ s/\?/$_/;
    } else {
      $sql =~ s/\?/'$_'/;
    }
  }
  $self->trace(sprintf "SQL (? resolved):\n%s\nARGS:\n%s\n",
      $sql, Data::Dumper::Dumper(\@arguments));
  $self->create_commandfile($sql);
  my $exit_output = `$self->{sqsh}`;
  if ($?) {
    printf STDERR "fetchrow_array exit bumm \n";
    my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; <> };
    my @oerrs = map {
      /(ORA\-\d+:.*)/ ? $1 : ();
    } split(/\n/, $output);
    $self->{errstr} = join(" ", @oerrs);
  } else {
    my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; <> };
    @row = map { convert($_) } 
        map { s/^\s+([\.\d]+)$/$1/g; $_ }         # strip leading space from numbers
        map { s/\s+$//g; $_ }                     # strip trailing space
        split(/\|/, (split(/\n/, $output))[0]);
    $self->trace(sprintf "RESULT:\n%s\n",
        Data::Dumper::Dumper(\@row));
  }
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
  }
  unlink $self->{sql_commandfile};
  unlink $self->{sql_resultfile};
  return $row[0] unless wantarray;
  return @row;
}

sub fetchall_array {
  my $self = shift;
  my $sql = shift;
  my @arguments = @_;
  my $sth = undef;
  my $rows = undef;
  foreach (@arguments) {
    # replace the ? by the parameters
    if (/^\d+$/) {
      $sql =~ s/\?/$_/;
    } else {
      $sql =~ s/\?/'$_'/;
    }
  }
  $self->trace(sprintf "SQL (? resolved):\n%s\nARGS:\n%s\n",
      $sql, Data::Dumper::Dumper(\@arguments));
  $self->create_commandfile($sql);
  my $exit_output = `$self->{sqsh}`;
  if ($?) {
    printf STDERR "fetchrow_array exit bumm %s\n", $exit_output;
    my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; <> };
    my @oerrs = map {
      /(ORA\-\d+:.*)/ ? $1 : ();
    } split(/\n/, $output);
    $self->{errstr} = join(" ", @oerrs);
  } else {
    my $output = do { local (@ARGV, $/) = $self->{sql_resultfile}; <> };
    my @rows = map { [ 
        map { convert($_) } 
        map { s/^\s+([\.\d]+)$/$1/g; $_ }
        map { s/\s+$//g; $_ }
        split /\|/
    ] } grep { ! /^\(\d+ rows affected\)/ } 
        grep { ! /^\s*$/ }
        grep { ! /^Database name .* ignored, referencing object in/ } split(/\n/, $output);
    $rows = \@rows;
    $self->trace(sprintf "RESULT:\n%s\n",
        Data::Dumper::Dumper($rows));
  }
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
  }
  unlink $self->{sql_commandfile};
  unlink $self->{sql_resultfile};
  return @{$rows};
}

sub func {
  my $self = shift;
  my $function = shift;
  $self->{handle}->func(@_);
}

sub convert {
  my $n = shift;
  # mostly used to convert numbers in scientific notation
  if ($n =~ /^\s*\d+\s*$/) {
    return $n;
  } elsif ($n =~ /^\s*([-+]?)(\d*[\.,]*\d*)[eE]{1}([-+]?)(\d+)\s*$/) {
    my ($vor, $num, $sign, $exp) = ($1, $2, $3, $4);
    $n =~ s/E/e/g;
    $n =~ s/,/\./g;
    $num =~ s/,/\./g;
    my $sig = $sign eq '-' ? "." . ($exp - 1 + length $num) : '';
    my $dec = sprintf "%${sig}f", $n;
    $dec =~ s/\.[0]+$//g;
    return $dec;
  } elsif ($n =~ /^\s*([-+]?)(\d+)[\.,]*(\d*)\s*$/) {
    return $1.$2.".".$3;
  } elsif ($n =~ /^\s*(.*?)\s*$/) {
    return $1;
  } else {
    return $n;
  }
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
  $self->trace("try to clean up command and result files");
  unlink $self->{sql_commandfile} if -f $self->{sql_commandfile};
  unlink $self->{sql_resultfile} if -f $self->{sql_resultfile};
}

sub create_commandfile {
  my $self = shift;
  my $sql = shift;
  open CMDCMD, "> $self->{sql_commandfile}";
  printf CMDCMD "%s\n", $sql;
  printf CMDCMD "go\n";
  close CMDCMD;
}

package DBD::MSSQL::Server::Connection::Sqlrelay;

use strict;
use Net::Ping;

our @ISA = qw(DBD::MSSQL::Server::Connection);


sub init {
  my $self = shift;
  my %params = @_;
  my $retval = undef;
  if ($self->{mode} =~ /^server::tnsping/) {
  } else {
    if (! $self->{hostname} || ! $self->{username} || ! $self->{password} || ! $self->{port}) {
      $self->{errstr} = "Please specify database, username and password";
      return undef;
    }
  }
  if (! exists $self->{errstr}) {
    my $stderrvar;
    eval {
      require DBI;
      use POSIX ':signal_h';
      if ($^O =~ /MSWin/) {
        local $SIG{'ALRM'} = sub {
          die "alarm\n";
        };
      } else {
        my $mask = POSIX::SigSet->new( SIGALRM );
        my $action = POSIX::SigAction->new(
            sub { die "alarm\n" ; }, $mask);
        my $oldaction = POSIX::SigAction->new();
        sigaction(SIGALRM ,$action ,$oldaction );
      }
      alarm($self->{timeout} - 1); # 1 second before the global unknown timeout
      *SAVEERR = *STDERR;
      open OUT ,'>',\$stderrvar;
      *STDERR = *OUT;
      if ($self->{handle} = DBI->connect(
          #sprintf("DBI:SQLRelay:host=%s;port=%d;socket=%s", 
          sprintf("DBI:SQLRelay:host=%s;port=%d;",
              $self->{hostname}, $self->{port}),
        $self->{username},
        $self->{password},
        { RaiseError => 1, AutoCommit => 0, PrintError => 1 })) {
      } else {
        $self->{errstr} = DBI::errstr();
      }
      my $answer = $self->fetchrow_array(
          q{ SELECT 42 });
      die $self->{errstr} unless defined $answer and $answer == 42;
      *STDERR = *SAVEERR;
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
  my $new_dbh = $self->{handle}->clone();
  $self->trace(sprintf "fetchrow_array: %s", $sql);
  #
  # does not work with bind variables
  #
  while ($sql =~ /\?/) {
    my $param = shift @arguments;
    if ($param !~ /^\d+$/) {
      $param = $self->{handle}->quote($param);
    }
    $sql =~ s/\?/$param/;
  }
  $sql =~ s/^\s*//g;
  $sql =~ s/\s*$//g;
  eval {
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    @row = $sth->fetchrow_array();
    $sth->finish();
  };
  if ($@) {
    $self->debug(sprintf "bumm %s", $@);
  }
  # without this trick, there are error messages like
  # "No server-side cursors were available to process the query"
  # and the results are messed up.
  $self->{handle}->disconnect();
  $self->{handle} = $new_dbh;
  if (-f "/tmp/check_mssql_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) =
        "/tmp/check_mssql_health_simulation/".$self->{mode}; <> };
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
  my $new_dbh = $self->{handle}->clone();
  $self->trace(sprintf "fetchall_array: %s", $sql);
  while ($sql =~ /\?/) {
    my $param = shift @arguments;
    if ($param !~ /^\d+$/) {
      $param = $self->{handle}->quote($param);
    }
    $sql =~ s/\?/$param/;
  }
  eval {
    $sth = $self->{handle}->prepare($sql);
    if (scalar(@arguments)) {
      $sth->execute(@arguments);
    } else {
      $sth->execute();
    }
    $rows = $sth->fetchall_arrayref();
    my $asrows = $sth->fetchall_arrayref();
    $sth->finish();
  };
  if ($@) {
    printf STDERR "bumm %s\n", $@;
  }
  $self->{handle}->disconnect();
  $self->{handle} = $new_dbh;
  if (-f "/tmp/check_mssql_health_simulation/".$self->{mode}) {
    my $simulation = do { local (@ARGV, $/) =
        "/tmp/check_mssql_health_simulation/".$self->{mode}; <> };
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
      } elsif ($line =~ /(.*?)\s*=\s*(.*)/) {
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

$PROGNAME = "check_mssql_health";
$REVISION = '$Revision: 1.5.19 $';
$CONTACT = 'gerhard.lausser@consol.de';
$TIMEOUT = 60;
$STATEFILESDIR = '/var/tmp/check_mssql_health';
$needs_restart = 0;

my @modes = (
  ['server::connectiontime',
      'connection-time', undef,
      'Time to connect to the server' ],
  ['server::cpubusy',
      'cpu-busy', undef,
      'Cpu busy in percent' ],
  ['server::iobusy',
      'io-busy', undef,
      'IO busy in percent' ],
  ['server::fullscans',
      'full-scans', undef,
      'Full table scans per second' ],
  ['server::connectedusers',
      'connected-users', undef,
      'Number of currently connected users' ],
  ['server::database::transactions',
      'transactions', undef,
      'Transactions per second (per database)' ],
  ['server::batchrequests',
      'batch-requests', undef,
      'Batch requests per second' ],
  ['server::latch::waits',
      'latches-waits', undef,
      'Number of latch requests that could not be granted immediately' ],
  ['server::latch::waittime',
      'latches-wait-time', undef,
      'Average time for a latch to wait before the request is met' ],
  ['server::memorypool::lock::waits',
      'locks-waits', undef,
      'The number of locks per second that had to wait' ],
  ['server::memorypool::lock::timeouts',
      'locks-timeouts', undef,
      'The number of locks per second that timed out' ],
  ['server::memorypool::lock::deadlocks',
      'locks-deadlocks', undef,
      'The number of deadlocks per second' ],
  ['server::sql::recompilations',
      'sql-recompilations', undef,
      'Re-Compilations per second' ],
  ['server::sql::initcompilations',
      'sql-initcompilations', undef,
      'Initial compilations per second' ],
  ['server::totalmemory',
      'total-server-memory', undef,
      'The amount of memory that SQL Server has allocated to it' ],
  ['server::memorypool::buffercache::hitratio',
      'mem-pool-data-buffer-hit-ratio', ['buffer-cache-hit-ratio'],
      'Data Buffer Cache Hit Ratio' ],


  ['server::memorypool::buffercache::lazywrites',
      'lazy-writes', undef,
      'Lazy writes per second' ],
  ['server::memorypool::buffercache::pagelifeexpectancy',
      'page-life-expectancy', undef,
      'Seconds a page is kept in memory before being flushed' ],
  ['server::memorypool::buffercache::freeliststalls',
      'free-list-stalls', undef,
      'Requests per second that had to wait for a free page' ],
  ['server::memorypool::buffercache::checkpointpages',
      'checkpoint-pages', undef,
      'Dirty pages flushed to disk per second. (usually by a checkpoint)' ],


  ['server::database::online',
      'database-online', undef,
      'Check if a database is online and accepting connections' ],
  ['server::database::databasefree',
      'database-free', undef,
      'Free space in database' ],
  ['server::database::backupage',
      'database-backup-age', ['backup-age'],
      'Elapsed time (in hours) since a database was last backed up' ],
  ['server::database::logbackupage',
      'database-logbackup-age', ['logbackup-age'],
      'Elapsed time (in hours) since a database transaction log was last backed up' ],
  ['server::database::autogrowths::file',
      'database-file-auto-growths', undef,
      'The number of File Auto Grow events (either data or log) in the last <n> minutes (use --lookback)' ],
  ['server::database::autogrowths::logfile',
      'database-logfile-auto-growths', undef,
      'The number of Log File Auto Grow events in the last <n> minutes (use --lookback)' ],
  ['server::database::autogrowths::datafile',
      'database-datafile-auto-growths', undef,
      'The number of Data File Auto Grow events in the last <n> minutes (use --lookback)' ],
  ['server::database::autoshrinks::file',
      'database-file-auto-shrinks', undef,
      'The number of File Auto Shrink events (either data or log) in the last <n> minutes (use --lookback)' ],
  ['server::database::autoshrinks::logfile',
      'database-logfile-auto-shrinks', undef,
      'The number of Log File Auto Shrink events in the last <n> minutes (use --lookback)' ],
  ['server::database::autoshrinks::datafile',
      'database-datafile-auto-shrinks', undef,
      'The number of Data File Auto Shrink events in the last <n> minutes (use --lookback)' ],
  ['server::database::dbccshrinks::file',
      'database-file-dbcc-shrinks', undef,
      'The number of DBCC File Shrink events (either data or log) in the last <n> minutes (use --lookback)' ],

  ['server::jobs::failed',
      'failed-jobs', undef,
      'The number of jobs which did not exit successful in the last <n> minutes (use --lookback)' ],

  ['server::sql',
      'sql', undef,
      'any sql command returning a single number' ],
  ['server::sqlruntime',
      'sql-runtime', undef,
      'the time an sql command needs to run' ],

  ['server::database::listdatabases',
      'list-databases', undef,
      'convenience function which lists all databases' ],
  ['server::database::datafile::listdatafiles',
      'list-datafiles', undef,
      'convenience function which lists all datafiles' ],
  ['server::memorypool::lock::listlocks',
      'list-locks', undef,
      'convenience function which lists all locks' ],
);

sub print_usage () {
  print <<EOUS;
  Usage:
    $PROGNAME [-v] [-t <timeout>] --hostname=<db server hostname>
        --username=<username> --password=<password> [--port <port>]
        --mode=<mode>
    $PROGNAME [-v] [-t <timeout>] --server=<db server>
        --username=<username> --password=<password>
        --mode=<mode>
    $PROGNAME [-h | --help]
    $PROGNAME [-V | --version]

  Options:
    --hostname
       the database server
    --port
       the database server's port
    --server
       the name of a predefined connection
    --currentdb
       the name of a database which is used as the current database
       for the connection. (don't use this parameter unless you
       know what you're doing)
    --username
       the mssql user
    --password
       the mssql user's password
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
       the name of the database etc depending on the mode.
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
    --offlineok
       if mode database-free finds a database which is currently offline,
       a WARNING is issued. If you don't want this and if offline databases
       are perfectly ok for you, then add --offlineok. You will get OK instead.

  Database-related modes check all databases in one run by default.
  If only a single database should be checked, use the --name parameter.
  The same applies to datafile-related modes.
  If an additional --regexp is added, --name's argument will be interpreted
  as a regular expression.
  The parameter --mitigation lets you classify the severity of an offline
  tablespace. 

  In mode sql you can url-encode the statement so you will not have to mess
  around with special characters in your Nagios service definitions.
  Instead of 
  --name="select count(*) from master..sysprocesses"
  you can say 
  --name=select%20count%28%2A%29%20from%20master%2E%2Esysprocesses
  For your convenience you can call check_mssql_health with the --encode
  option and it will encode the standard input.

  You can find the full documentation for this plugin at
  http://labs.consol.de/nagios/check_mssql_health or


EOUS
#
# --basis
#  one of rate, delta, value
  
}

sub print_help () {
  print "Copyright (c) 2009 Gerhard Lausser\n\n";
  print "\n";
  print "  Check various parameters of MSSQL databases \n";
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
    "server=s",
    "currentdb=s",
    "mode|m=s",
    "tablespace=s",
    "database=s",
    "datafile=s",
    "waitevent=s",
    "offlineok",
    "mitigation=s",
    "notemp",
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
    "statefilesdir=s",
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
  $DBD::MSSQL::Server::verbose = exists $commandline{verbose};
}

if (exists $commandline{scream}) {
#  $DBD::MSSQL::Server::hysterical = exists $commandline{scream};
}

if (exists $commandline{report}) {
  # short, long, html
} else {
  $commandline{report} = "long";
}

if (exists $commandline{'with-mymodules-dyn-dir'}) {
  $DBD::MSSQL::Server::my_modules_dyn_dir = $commandline{'with-mymodules-dyn-dir'};
} else {
  $DBD::MSSQL::Server::my_modules_dyn_dir = '/usr/local/nagios/libexec';
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
          if $DBD::MSSQL::Server::verbose;
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
  foreach my $important_env (qw(LD_LIBRARY_PATH SHLIB_PATH 
      MSSQL_HOME TNS_ADMIN ORA_NLS ORA_NLS33 ORA_NLS10)) {
    if ($ENV{$important_env} && ! scalar(grep { /^$important_env=/ } 
        keys %{$commandline{environment}})) {
      $commandline{environment}->{$important_env} = $ENV{$important_env};
      printf STDERR "add important --environment %s=%s\n", 
          $important_env, $ENV{$important_env} if $DBD::MSSQL::Server::verbose;
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

if (exists $commandline{mitigation}) {
  if ($commandline{mitigation} eq "ok") {
    $commandline{mitigation} = 0;
  } elsif ($commandline{mitigation} eq "warning") {
    $commandline{mitigation} = 1;
  } elsif ($commandline{mitigation} eq "critical") {
    $commandline{mitigation} = 2;
  } elsif ($commandline{mitigation} eq "unknown") {
    $commandline{mitigation} = 3;
  }
}

if (exists $commandline{shell}) {
  # forget what you see here.
  system("/bin/sh");
}

if (! exists $commandline{statefilesdir}) {
  if (exists $ENV{OMD_ROOT}) {
    $commandline{statefilesdir} = $ENV{OMD_ROOT}."/var/tmp/check_mssql_health";
  } else {
    $commandline{statefilesdir} = $STATEFILESDIR;
  }
}

if (exists $commandline{name}) {
  if ($^O =~ /MSWin/ && $commandline{name} =~ /^'(.*)'$/) {
    # putting arguments in single ticks under Windows CMD leaves the ' intact
    # we remove them
    $commandline{name} = $1;
  }
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
    method => $commandline{method} ||
        $ENV{NAGIOS__SERVICEMSSQL_METH} ||
        $ENV{NAGIOS__HOSTMSSQL_METH} || 'dbi',
    hostname => $commandline{hostname}  || 
        $ENV{NAGIOS__SERVICEMSSQL_HOST} ||
        $ENV{NAGIOS__HOSTMSSQL_HOST},
    username => $commandline{username} || 
        $ENV{NAGIOS__SERVICEMSSQL_USER} ||
        $ENV{NAGIOS__HOSTMSSQL_USER},
    password => $commandline{password} || 
        $ENV{NAGIOS__SERVICEMSSQL_PASS} ||
        $ENV{NAGIOS__HOSTMSSQL_PASS},
    port => $commandline{port} || 
        $ENV{NAGIOS__SERVICEMSSQL_PORT} ||
        $ENV{NAGIOS__HOSTMSSQL_PORT},
    server => $commandline{server}  || 
        $ENV{NAGIOS__SERVICEMSSQL_SERVER} ||
        $ENV{NAGIOS__HOSTMSSQL_SERVER},
    currentdb => $commandline{currentdb}  || 
        $ENV{NAGIOS__SERVICEMSSQL_CURRENTDB} ||
        $ENV{NAGIOS__HOSTMSSQL_CURRENTDB},
    warningrange => $commandline{warning},
    criticalrange => $commandline{critical},
    dbthresholds => $commandline{dbthresholds},
    absolute => $commandline{absolute},
    lookback => $commandline{lookback} || 30,
    tablespace => $commandline{tablespace},
    database => $commandline{database},
    datafile => $commandline{datafile},
    offlineok => $commandline{offlineok},
    mitigation => $commandline{mitigation},
    basis => $commandline{basis},
    offlineok => $commandline{offlineok},
    mitigation => $commandline{mitigation},
    notemp => $commandline{notemp},
    selectname => $commandline{name} || $commandline{tablespace} || $commandline{datafile},
    regexp => $commandline{regexp},
    name => $commandline{name},
    name2 => $commandline{name2} || $commandline{name},
    units => $commandline{units},
    eyecandy => $commandline{eyecandy},
    statefilesdir => $commandline{statefilesdir},
    verbose => $commandline{verbose},
    report => $commandline{report},
);

my $server = undef;

$server = DBD::MSSQL::Server->new(%params);
$server->nagios(%params);
$server->calculate_result();
$nagios_message = $server->{nagios_message};
$nagios_level = $server->{nagios_level};
$perfdata = $server->{perfdata};

printf "%s - %s", $ERRORCODES{$nagios_level}, $nagios_message;
printf " | %s", $perfdata if $perfdata;
printf "\n";
exit $nagios_level;
