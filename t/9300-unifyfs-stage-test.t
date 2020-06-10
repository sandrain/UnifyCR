#!/bin/bash
#
# Test unifyfs-stage executable for basic functionality
#

test_description="Test basic functionality of unifyfs-stage executable"

. $(dirname $0)/sharness.sh

#test_expect_success "Intercepted mount point $UNIFYFS_MOUNTPOINT is empty" '
#    test_dir_is_empty $UNIFYFS_MOUNTPOINT
#'

test_expect_success "unifyfs-stage exists" '
    test_path_is_file ${SHARNESS_BUILD_DIRECTORY}/util/unifyfs-stage/src/unifyfs-stage
'
test_expect_success "testing temp dir exists" '
    test_path_is_dir  ${UNIFYFS_TEST_TMPDIR} 
'

mkdir ${UNIFYFS_TEST_TMPDIR}/config
mkdir ${UNIFYFS_TEST_TMPDIR}/stage_source
mkdir ${UNIFYFS_TEST_TMPDIR}/stage_destination

test_expect_success "stage testing dirs exist" '
    test_path_is_dir  ${UNIFYFS_TEST_TMPDIR}/config
    test_path_is_dir  ${UNIFYFS_TEST_TMPDIR}/stage_source
    test_path_is_dir  ${UNIFYFS_TEST_TMPDIR}/stage_destination
'    

dd bs=1000000 if=/dev/zero of=${UNIFYFS_TEST_TMPDIR}/stage_source/source.file count=5

test_expect_success "source.file exists (sourced from /dev/zero)" '
    test_path_is_file ${UNIFYFS_TEST_TMPDIR}/stage_source/source.file
'

test_done
