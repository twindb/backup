def test_basename(s3):
    assert s3.basename('s3://test-bucket/some_dir/some_file.txt') == \
        'some_dir/some_file.txt'
