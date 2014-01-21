#!/usr/bin/python -u
'''
    ADOdb version update script

    Updates the version number, date and copyright year in
    all php, txt and htm files
'''

from datetime import date
import getopt
import os
from os import path
import re
import subprocess
import sys

# ADOdb version validation regex
_version_dev = "dev"
_version_regex = "[Vv]?[0-9]\.[0-9]+([a-z]|%s)?" % _version_dev
_release_date_regex = "[0-9?]+.*[0-9]+"

# Command-line options
options = "hc"
long_options = ["help", "commit"]


def usage():
    print '''Usage: %s version

    Parameters:
        version                 ADOdb version, format: [v]X.YY[a-z|dev]

    Options:
        -c | --commit           Automatically commits the changes
        -h | --help             Show this usage message
''' % (
        path.basename(__file__)
    )
#end usage()


def version_check(version):
    ''' Checks that the given version is valid, exits with error if not.
        Returns the version without the "v" prefix
    '''
    if not re.search("^%s$" % _version_regex, version):
        usage()
        print "ERROR: invalid version ! \n"
        sys.exit(1)

    return version.lstrip("Vv")


def release_date(version):
    ''' Returns the release date in DD-MMM-YYYY format
        For development releases, DD-MMM will be ??-???
    '''
    # Development release
    if version.endswith(_version_dev):
        date_format = "??-???-%Y"
    else:
        date_format = "%d-%b-%Y"

    # Define release date
    return date.today().strftime(date_format)


def sed_script(version):
    ''' Builds sed script to update version information in source files
    '''
    copyright_string = "\(c\)"

    # - Part 1: version number and release date
    script = "s/%s\s+%s\s+(%s)/V%s  %s  \\2/\n" % (
        _version_regex,
        _release_date_regex,
        copyright_string,
        version,
        release_date(version)
    )

    # - Part 2: copyright year
    script += "s/(%s)\s*%s(.*Lim)/\\1 \\2-%s\\3/" % (
        copyright_string,
        "([0-9]+)-[0-9]+",      # copyright years
        date.today().strftime("%Y")
    )

    return script


def sed_filelist():
    ''' Build list of files to update
    '''
    def sed_filter(name):
        return name.lower().endswith((".php", ".htm", ".txt"))

    dirlist = []
    for root, dirs, files in os.walk(".", topdown=True):
        for name in filter(sed_filter, files):
            dirlist.append(path.join(root, name))

    return dirlist


def version_set(version, do_commit):
    ''' Bump version number and set release date in source files
    '''
    print "Updating version and date in source files"
    subprocess.call(
        "sed -r -i '%s' %s " % (
            sed_script(version),
            " ".join(sed_filelist())
        ),
        shell=True
    )
    print "Version set to %s" % version

    if do_commit:
        # Commit changes
        print "Committing"
        result = subprocess.call(
            "git commit --all --message '%s'" % (
                "Bump version to %s" % version
            ),
            shell=True
        )

        if result == 0:
            print '''
NOTE: you should carefully review the new commit, making sure updates
to the files are correct and no additional changes are required.
If everything is fine, then the commit can be pushed upstream;
otherwise:
 - Make the required corrections
 - Amend the commit ('git commit --all --amend' ) or create a new one
'''
    else:
        print "Note: changes have been staged but not committed."
#end version_set()


def main():
    # Get command-line options
    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], options, long_options)
    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(2)

    if len(args) < 1:
        usage()
        print "ERROR: please specify the version"
        sys.exit(1)

    do_commit = False

    for opt, val in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit(0)

        elif opt in ("-c", "--commit"):
            do_commit = True

    # Mandatory parameters
    version = version_check(args[0])

    # Let's do it
    version_set(version, do_commit)
#end main()


if __name__ == "__main__":
    main()