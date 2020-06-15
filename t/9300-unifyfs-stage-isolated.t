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

mkdir -p ${UNIFYFS_TEST_TMPDIR}/config_9300
mkdir -p ${UNIFYFS_TEST_TMPDIR}/stage_source
mkdir -p ${UNIFYFS_TEST_TMPDIR}/stage_destination_9300

test_expect_success "stage testing dirs exist" '
    test_path_is_dir  ${UNIFYFS_TEST_TMPDIR}/config_9300
    test_path_is_dir  ${UNIFYFS_TEST_TMPDIR}/stage_source
    test_path_is_dir  ${UNIFYFS_TEST_TMPDIR}/stage_destination_9300
'    

dd bs=1000000 if=/dev/zero of=${UNIFYFS_TEST_TMPDIR}/stage_source/source.file count=5

test_expect_success "source.file exists (sourced from /dev/zero)" '
    test_path_is_file ${UNIFYFS_TEST_TMPDIR}/stage_source/source.file
'

rm -f ${UNIFYFS_TEST_TMPDIR}/config_9300/*
rm -f ${UNIFYFS_TEST_TMPDIR}/stage_destination_9300/*

test_expect_success "config_9300 directory is empty" '
    test_dir_is_empty ${UNIFYFS_TEST_TMPDIR}/config_9300
'

echo "\"${UNIFYFS_TEST_TMPDIR}/stage_source/source.file\" \"${UNIFYFS_TEST_TMPDIR}/stage_destination_9300/destination_9300.file\""  > ${UNIFYFS_TEST_TMPDIR}/config_9300/test_INOUT.manifest

test_expect_success "config_9300 directory now has manifest files" '
    test_path_is_file  ${UNIFYFS_TEST_TMPDIR}/config_9300/test_INOUT.manifest

'

test_expect_success "target directory is empty" '
    test_dir_is_empty ${UNIFYFS_TEST_TMPDIR}/stage_destination_9300
'

#${SHARNESS_BUILD_DIRECTORY}/util/unifyfs-stage/src/unifyfs-stage -m ${UNIFYFS_TEST_MOUNT} ${UNIFYFS_TEST_TMPDIR}/config_9300/test_INOUT.manifest > ${UNIFYFS_TEST_TMPDIR}/config_9300/stage_INOUT_output.OUT 2>&1 
#${SHARNESS_BUILD_DIRECTORY}/util/unifyfs-stage/src/unifyfs-stage -m ${UNIFYFS_TEST_MOUNT} ${UNIFYFS_TEST_TMPDIR}/config_9300/test_INOUT.manifest > ${UNIFYFS_TEST_TMPDIR}/config_9300/stage_INOUT_output.OUT 2>&1 
#${SHARNESS_BUILD_DIRECTORY}/util/unifyfs-stage/src/unifyfs-stage -m ${UNIFYFS_TEST_MOUNT} ${UNIFYFS_TEST_TMPDIR}/config_9300/test_OUT.manifest > ${UNIFYFS_TEST_TMPDIR}/config_9300/stage_OUT_output.OUT 2>&1 

${SHARNESS_BUILD_DIRECTORY}/util/unifyfs-stage/src/unifyfs-stage -N ${UNIFYFS_TEST_TMPDIR}/config_9300/test_INOUT.manifest > ${UNIFYFS_TEST_TMPDIR}/config_9300/stage_INOUT_output.OUT 2>&1 

test_expect_success "input file has been staged to output" '
    test_path_is_file ${UNIFYFS_TEST_TMPDIR}/stage_destination_9300/destination_9300.file
'

test_done
