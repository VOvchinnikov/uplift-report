import os
import s3fs
import pandas as pd
from datetime import datetime


def path(customer, audience):
    return "s3://remerge-customers/{0}/uplift_data/{1}".format(customer, audience)


def extract_revenue_events(df, revenue_event):
    return df[df.partner_event == revenue_event]


# helper to download CSV files, convert to DF and print time needed
# caches files locally and on S3 to be reused
def read_csv(customer, audience, revenue_event, source, date, chunk_filter_fn=None, chunk_size=10 ** 6):
    now = datetime.now()

    date_str = date.strftime('%Y%m%d')

    filename = '{0}/{1}/{2}.csv.gz'.format(path(customer, audience), source, date_str)

    # local cache
    cache_dir = 'cache/{0}/{1}'.format(audience, source)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    cache_filename = '{0}/{1}.parquet'.format(cache_dir, date_str)

    # s3 cache (useful if we don't have enough space on the Colab instance)
    s3_cache_filename = '{0}/{1}/cache/{2}.parquet'.format(path(customer, audience),
                                                           source, date_str)

    if source == 'attributions':
        cache_filename = '{0}/{1}-{2}.parquet'.format(cache_dir, date_str,
                                                      revenue_event)

        # s3 cache (useful if we don't have enough space on the Colab instance)
        s3_cache_filename = '{0}/{1}/cache/{2}-{3}.parquet' \
            .format(path(customer, audience), source, date_str, revenue_event)

    if os.path.exists(cache_filename):
        print(now, 'loading from', cache_filename)
        return pd.read_parquet(cache_filename, engine='pyarrow')

    fs = s3fs.S3FileSystem(anon=False)
    fs.connect_timeout = 10  # defaults to 5
    fs.read_timeout = 30  # defaults to 15 

    if fs.exists(path=s3_cache_filename):
        print(now, 'loading from S3 cache', s3_cache_filename)

        # Download the file to local cache first to avoid timeouts during the load.
        # This way, if they happen, restart will be using local copies first.
        fs.get(s3_cache_filename, cache_filename)

        print(now, 'stored S3 cache file to local drive, loading', cache_filename)

        return pd.read_parquet(cache_filename, engine='pyarrow')

    print(now, 'start loading CSV for', audience, source, date)

    read_csv_kwargs = {'chunksize': chunk_size}

    if source == 'attributions':
        # Only read the columns that are going to be used from attribution
        read_csv_kwargs['usecols'] = ['ts', 'user_id', 'partner_event',
                                      'revenue_eur', 'ab_test_group']

    df = pd.DataFrame()

    if not fs.exists(path=filename):
        print(now, 'WARNING: no CSV file at for: ', audience, source, date, ', skipping the file: ', filename)
        return df

    for chunk in pd.read_csv(filename, escapechar='\\', low_memory=False,
                             **read_csv_kwargs):
        if chunk_filter_fn:
            filtered_chunk = chunk_filter_fn(chunk)
        else:
            filtered_chunk = chunk

        df = pd.concat([df, filtered_chunk],
                       ignore_index=True, verify_integrity=True)

    print(datetime.now(), 'finished loading CSV for', date.strftime('%d.%m.%Y'),
          'took', datetime.now() - now)

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    df.to_parquet(cache_filename, engine='pyarrow')

    # write it to the S3 cache folder as well
    print(datetime.now(), 'caching as parquet', s3_cache_filename)

    df.to_parquet(s3_cache_filename, engine='pyarrow')
    return df
