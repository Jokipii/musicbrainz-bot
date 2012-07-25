CREATE TABLE update_log
(
	id serial NOT NULL,
	updated varchar(64) NOT NULL,
	event text,
	timest timestamp DEFAULT now(),
	CONSTRAINT l_update_log_pkey PRIMARY KEY (id)
);

CREATE INDEX releases_extraartists_idx_release_id ON releases_extraartists USING btree (release_id);
CREATE INDEX releases_artists_idx_release_id ON releases_artists USING btree (release_id);
CREATE INDEX releases_labels_idx_release_id ON releases_labels USING btree (release_id);
CREATE INDEX releases_formats_idx_release_id ON releases_formats USING btree (release_id);
CREATE INDEX release_identfier_idx_release_id ON release_identifier USING btree (release_id);

CREATE INDEX track_idx_release_id ON track USING btree (release_id);
CREATE INDEX tracks_extraartists_idx_track_id ON tracks_extraartists USING btree (track_id);
CREATE INDEX tracks_artists_idx_track_id ON tracks_artists USING btree (track_id);

CREATE INDEX mb_artist_link_idx_id ON mb_artist_link USING btree (id);
CREATE INDEX mb_release_link_idx_id ON mb_release_link USING btree (id);
CREATE INDEX mb_label_link_idx_id ON mb_label_link USING btree (id);
CREATE INDEX mb_release_group_link_idx_id ON mb_release_group_link USING btree (id);
