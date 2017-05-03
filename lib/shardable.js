class Shardable {

	constructor(knex) {
		this.knex = knex;
	}

	createNextIdFunction(schemaName = 'public') {

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
		return this.knex.raw(sql);
	}

	dropNextIdFunction(schemaName = 'public') {
		const sql = `DROP FUNCTION ${schemaName}.next_id(In seq_name regclass, set_shard_id int, OUT result bigint);`
		return this.knex.raw(sql);
	}


	setShardPrimaryKey(tableName, owner = null, shardId = 1) {

		let user;
		if(owner === null) {
			user = this.knex.client.config.connection.user;
		} else {
			user = owner;
		}

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

		return this.knex.raw(sql);
	}

	dropSequence(tableName) {
		const sql = `DROP SEQUENCE ${tableName}_id_seq;`
		return this.knex.raw(sql);
	}
}


exports.default = Shardable;
module.exports = exports['default'];