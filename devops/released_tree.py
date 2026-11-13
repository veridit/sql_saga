#!/usr/bin/env python3
"""Historical source trees for the specification tests of released update-script generators.

The 1.4 and 1.5 generators build their release's update scripts from that release's src/. After a later release
changes src/, their tests run against the released tree, taken from the git tag with `git archive`, so they keep
specifying the generator exactly as released. Standard library only.
"""
import atexit
import os
import shutil
import subprocess
import tarfile
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
_TREES = {}


def released_tree(tag):
    """A private extracted copy of src/ and devops/ at tag, removed at interpreter exit."""
    if tag not in _TREES:
        os.makedirs(os.path.join(ROOT, 'tmp'), exist_ok=True)
        target = tempfile.mkdtemp(prefix='released_%s_' % tag, dir=os.path.join(ROOT, 'tmp'))
        atexit.register(shutil.rmtree, target, True)
        archive = subprocess.run(['git', '-C', ROOT, 'archive', '--format=tar', tag, 'src', 'devops'],
                                 check=True, capture_output=True).stdout
        with tempfile.TemporaryFile() as f:
            f.write(archive)
            f.seek(0)
            with tarfile.open(fileobj=f) as tar:
                try:
                    tar.extractall(target, filter='data')
                except TypeError:  # Python without extraction filters; the archive is our own git tree
                    tar.extractall(target)
        _TREES[tag] = target
    return _TREES[tag]


def released_src(tag):
    return os.path.join(released_tree(tag), 'src')


def released_devops(tag):
    return os.path.join(released_tree(tag), 'devops')
