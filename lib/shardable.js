import Promise from 'bluebrid';


class Shardable {

	constructor(knex) {
		this.knex = knex;
	}

	set knex(knex) {
		this.knex = knex;
	}

	get knex(){
		return this.knex;
	}


	createNextIdFunction(schemaName = 'public') {
		return new Promise((resolve, reject) => {
			const sql = `
		CREATE OR REPLACE FUNCTION ${schemaName}.next_id(In seq_name regclass, set_shard_id int, OUT result bigint) AS $$
		DECLARE
			our_epoch bigint := 1314220021721;
			seq_id bigint;
			now_millis bigint;
			shard_id int := set_shard_id;
			mod_key bigint := 1024;
		BEGIN
			SELECT nextval(seq_name) % mod_key INTO seq_id;
			SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;
			result := (now_millis - our_epoch) << 23;
			result := result | (shard_id << 10);
			result := result | (seq_id);
		END
		$$ LANGUAGE PLPGSQL;
	`
			knex.raw(sql).then((results)=> resolve(results)).catch((err) => reject(err));
		})

	}

	dropNextIdFunction(schemaName = 'public') {
		return new Promise((resolve, reject) => {
			const sql = `DROP FUNCTION ${schemaName}.next_id(In seq_name regclass, set_shard_id int, OUT result bigint);`
			knex.raw(sql).then((results)=> resolve(results)).catch((err) => reject(err));
		});
	}


	setShardPrimaryKey(tableName, owner = null, shardId = 1) {
		return new Promise((resolve, reject) => {
			let user = owner === null ? knex.client.config.connection.user : owner;

			const sql = `
			CREATE SEQUENCE ${tableName}_id_seq
			  INCREMENT 1
			  MINVALUE 1
			  MAXVALUE 9223372036854775807
			  START 1
			  CACHE 1;
			ALTER TABLE ${tableName}_id_seq
			  OWNER TO ${user};
	
			ALTER TABLE ${tableName} ADD CONSTRAINT ${tableName}_pkey PRIMARY KEY(id);
			ALTER TABLE ${tableName} ALTER COLUMN id SET DEFAULT next_id('${tableName}_id_seq'::regclass, ${shardId});
			`
			knex.raw(sql).then((results)=> resolve(results)).catch((err) => reject(err));
		});
	}

	dropSequence(tableName) {
		return new Promise((resolve, reject) => {
			const sql = `DROP SEQUENCE ${tableName}_id_seq;`
			knex.raw(sql).then((results)=> resolve(results)).catch((err) => reject(err));
		});
	}
}

export default Shardable;