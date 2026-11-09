package db

import "testing"

func TestRoutingDefaultsAreInstalledOnceAndStorageIsPreserved(t *testing.T) {
	ctx, database := testDB(t)
	for _, stream := range []streamDoc{
		{ID: newID(), Name: "all", Title: "All messages", Description: "Default stream for every message", Routing: defaultRouting(), Storage: defaultStorage()},
		{ID: newID(), Name: "web-access", Title: "Web access", Routing: defaultRouting(), Storage: StorageConfig{MaxAgeHours: 72, MaxSizeMB: 200, MaxDocuments: 99, RetentionDays: 7}},
		{ID: newID(), Name: "system", Title: "System", Routing: defaultRouting(), Storage: defaultStorage()},
	} {
		if _, err := database.streams().InsertOne(ctx, stream); err != nil {
			t.Fatal(err)
		}
	}
	if err := UpgradeRoutingDefaults(ctx, database); err != nil {
		t.Fatal(err)
	}
	listed, err := NewStreamStore(database).List(ctx)
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
	if _, err := NewStreamStore(database).Save(ctx, stream, false); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeRoutingDefaults(ctx, database); err != nil {
		t.Fatal(err)
	}
	listed, err = NewStreamStore(database).List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range listed {
		if item.Name == "web-access" && item.Routing.Enabled {
			t.Fatal("restart overwrote user's disabled rule")
		}
	}
}

func TestRoutingDefaultsPreserveCustomRules(t *testing.T) {
	ctx, database := testDB(t)
	stream := validStream("web-access")
	stream.Routing = RoutingConfig{Enabled: true, Priority: 5, Match: "any", Rules: []RoutingRule{{Field: "source", Operator: "contains", Value: "custom-web"}}}
	if _, err := NewStreamStore(database).Save(ctx, stream, true); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeRoutingDefaults(ctx, database); err != nil {
		t.Fatal(err)
	}
	listed, err := NewStreamStore(database).List(ctx)
	if err != nil || len(listed) != 1 || listed[0].Routing.Priority != 5 || listed[0].Routing.Rules[0].Value != "custom-web" {
		t.Fatal(listed, err)
	}
}
