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
    await client.query(`CREATE TABLE IF NOT EXISTS gracilemigrations(
        id varchar(255) NOT NULL,
        updated_date date,
        CONSTRAINT migration_pkey PRIMARY KEY (id)
      );`)
  } catch (e) {
    console.log('init failed')
  } finally {
    client.release()
  }
}

const markMigrationQuery = id => [
  'INSERT INTO gracilemigrations(id) values ($1) RETURNING *',
  [id]
]
const existsQuery = id => [
  'select 1 from gracilemigrations where id = $1 limit 1;',
  [id]
]

export const transmit = async (id, migration) => {
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

    const applied = await client.query(...existsQuery(id))

    if (applied.rowCount > 0) {
      return // this migration already has been applied
    }

    // start transaction
    await client.query('BEGIN')

    try {
      for (const q of queries) {
        console.log('Apply:', q.toString())
        isString(q) ? await client.query(q) : await client.query(...q) // for safe dynamic queries like ['STATEMENT ?', X]
      }

      await client.query(...markMigrationQuery(id))

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
