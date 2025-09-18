CREATE TABLE "proposals" (
	"proposal_id" uuid PRIMARY KEY NOT NULL,
	"pluginAddress" varchar(42) NOT NULL,
	"creator" varchar(42) NOT NULL,
	"start_time" bigint NOT NULL,
	"end_time" bigint NOT NULL,
	"dao_address" text NOT NULL,
	"space_id" uuid NOT NULL
);
--> statement-breakpoint
ALTER TABLE "proposals" ADD CONSTRAINT "proposals_dao_address_spaces_dao_address_fk" FOREIGN KEY ("dao_address") REFERENCES "public"."spaces"("dao_address") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "proposals" ADD CONSTRAINT "proposals_space_id_spaces_id_fk" FOREIGN KEY ("space_id") REFERENCES "public"."spaces"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "proposals_space_id_idx" ON "proposals" USING btree ("space_id");--> statement-breakpoint
CREATE INDEX "proposals_creator_idx" ON "proposals" USING btree ("creator");--> statement-breakpoint
CREATE INDEX "proposals_start_time_idx" ON "proposals" USING btree ("start_time");--> statement-breakpoint
CREATE INDEX "proposals_end_time_idx" ON "proposals" USING btree ("end_time");--> statement-breakpoint
CREATE INDEX "proposals_space_time_idx" ON "proposals" USING btree ("space_id","start_time","end_time");