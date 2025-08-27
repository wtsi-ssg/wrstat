# wrstat
Efficiently walk a tree then get stats of all files in parallel across many
nodes using wr.

This is a replacement for https://github.com/wtsi-hgi/mpistat that doesn't use
any run-time inter-process communication, for greater reliability and speed.

It relies on https://github.com/VertebrateResequencing/wr to do the scheduling
of its work.

## Build

For normal use with a single executable:

```
git clone https://github.com/wtsi-ssg/wrstat/
cd wrstat
make install
```

To create 3 split executables so you can have particular permissions on each:

```
git clone https://github.com/wtsi-ssg/wrstat/
cd wrstat
make buildsplit
[then mv the executables to your PATH]
```

## Usage
As a user that has permission to see all files on the disks of interest:

```
wr manager start -s lsf
wrstat multi -w /working/dir -f /final/dir /disk1 /disk2
wr status -i wrstat -z -o s
```

Or if your user account has the ability to sudo without a password when
executing the wrstat executable, add the --sudo option to `wrstat multi`.
If you're running `wr` and `wrstat` without custom configurations, `wr` must
be run as the root user, because `wrstat` will generate `sudo` commands that
attempt to connect to a port number based on the user, which will be the root
user, not the running user.

NB: When running with sudo that is configured to not pass through environmental
variables, you must have a wr config file, accessible from the working
directory, with ManagerHost, ManagerPort, and ManagerCertDomain set.

For an increase in security by reducing attack surface area when using the sudo
option, use the wrstat-split* executables. wrstat-split can be run unprivileged
by a normal user to run the multi sub-command with the --sudo option; only
wrstat-split-walk and wrstat-split-stat need root. On Linux you can permit a
specific user to run only those binaries as root via sudoers, without giving
that user general sudo rights. Use visudo (or a file in /etc/sudoers.d) and add
entries like:

```
alice ALL=(root) NOPASSWD: /usr/local/bin/wrstat-split-walk, /usr/local/bin/wrstat-split-stat
```

- Use absolute paths that match your installation.
- Ensure the binaries are owned by root and not writable by the allowed user or
  group (e.g. chown root:root; chmod 0755).
- Optionally restrict exact arguments in sudoers for tighter control.

Only the specified user will be able to run those commands with sudo; other
users will be denied.

To do certain chmod and chown operations on desired paths to bring them in to
line with desired unix groups, create a YAML file like the example ch.yml in the
git repository, and supply it as the --ch option to `wrstat multi`.
For more details on ch, see `wrstat stat -h`.

Generally, use the `-h` option on wrstat and its sub commands for detailed
help.
