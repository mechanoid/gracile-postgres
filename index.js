import pg from 'pg'

const Pool = pg.Pool

const isString = item => typeof item === 'string'

let pool

export const init = async config => {
  console.log('init')
  pool = new Pool({
    max: config.poolSize || 20
  })

  pool.on('error', (err, client) => {
    console.error('Unexpected error on idle db client', err)
  })

  let client

  try {
    client = await pool.connect()
    await client.query(`CREATE TABLE IF NOT EXISTS "gracile-migrations" (
        id integer NOT NULL,
        updated_date date,
        CONSTRAINT migration_pkey PRIMARY KEY (id)
      );`)
  } catch (e) {
    console.log('init failed')
  } finally {
    client.release()
  }
}

export const transmit = async migration => {
  const querieOrQueries = migration()
  const queries =
    querieOrQueries && isString(querieOrQueries)
      ? [].concat(querieOrQueries)
      : querieOrQueries

  if (queries.length === 0) {
    return
  }

  let client
  try {
    client = await pool.connect()
    // start transaction
    await client.query('BEGIN')

    try {
      for (let i = 0; i < queries.length; i++) {
        const q = queries[i]
        isString(q) ? await client.query(q) : await client.query(...q) // for safe dynamic queries like ['STATEMENT ?', X]
      }

      // end transaction
      await client.query('COMMIT')
    } catch (e) {
      // rollback on error after begin of any transactions
      console.log('migration failed, rolling back')
      await client.query('ROLLBACK')
      throw e
    }
  } finally {
    client.release()
  }
}

export const close = async () => {
  pool.end()
}
