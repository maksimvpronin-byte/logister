package db

import "testing"

func TestRoutingDefaultsAreInstalledOnceAndStorageIsPreserved(t *testing.T) {
	ctx, tx := streamTransaction(t)
	_, err := tx.Exec(ctx, `INSERT INTO streams(name,title,description) VALUES ('all','All messages','Default stream for every message'),('web-access','Web access',''),('system','System','');
 UPDATE streams SET storage='{"max_age_hours":72,"max_size_mb":200,"max_documents":99,"retention_days":7}' WHERE name='web-access'`)
	if err != nil {
		t.Fatal(err)
	}
	if err := UpgradeRoutingDefaults(ctx, tx); err != nil {
		t.Fatal(err)
	}
	listed, err := NewStreamStore(tx).List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]Stream{}
	for _, stream := range listed {
		byName[stream.Name] = stream
	}
	if !byName["web-access"].Routing.Enabled || byName["web-access"].Routing.Rules[0].Value != "nginx" || byName["system"].Routing.Rules[0].Value != "cron" {
		t.Fatal(byName)
	}
	if byName["all"].Title != "Unmatched messages" || byName["web-access"].Storage.RetentionDays != 7 {
		t.Fatal("unrelated storage changed", byName)
	}
	stream := byName["web-access"]
	stream.Routing.Enabled = false
	if _, err := NewStreamStore(tx).Save(ctx, stream, false); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeRoutingDefaults(ctx, tx); err != nil {
		t.Fatal(err)
	}
	var enabled bool
	if err := tx.QueryRow(ctx, `SELECT (routing->>'enabled')::boolean FROM streams WHERE name='web-access'`).Scan(&enabled); err != nil || enabled {
		t.Fatal("restart overwrote user's disabled rule", enabled, err)
	}
}
func TestRoutingDefaultsPreserveCustomRules(t *testing.T) {
	ctx, tx := streamTransaction(t)
	stream := validStream("web-access")
	stream.Routing = RoutingConfig{Enabled: true, Priority: 5, Match: "any", Rules: []RoutingRule{{Field: "source", Operator: "contains", Value: "custom-web"}}}
	if _, err := NewStreamStore(tx).Save(ctx, stream, true); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeRoutingDefaults(ctx, tx); err != nil {
		t.Fatal(err)
	}
	listed, err := NewStreamStore(tx).List(ctx)
	if err != nil || len(listed) != 1 || listed[0].Routing.Priority != 5 || listed[0].Routing.Rules[0].Value != "custom-web" {
		t.Fatal(listed, err)
	}
}
