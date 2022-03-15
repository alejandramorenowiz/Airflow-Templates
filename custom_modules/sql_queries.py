class SqlQueries:

    songs_table_create = ("""
            CREATE TABLE IF NOT EXISTS public.songs (
                id_os varchar(256) NOT NULL,
                os varchar(256),
                CONSTRAINT os_pkey PRIMARY KEY (id_os)
            );
        """)


    songs_table_insert = ("""
            SELECT distinct id_review, os
            FROM log_reviews
        """)
        