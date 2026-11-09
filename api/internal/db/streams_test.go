package db

import "testing"

func validStream(name string) Stream {
	return Stream{Name: name, Title: "Payments", Routing: RoutingConfig{Priority: 10, Match: "all", Rules: []RoutingRule{}}, Storage: StorageConfig{MaxAgeHours: 24, MaxDocuments: 2}}
}

func TestStreamSettingsValidation(t *testing.T) {
	for _, name := range []string{"payments", "web-access", "team_1"} {
		if err := ValidateStream(validStream(name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, change := range []func(*Stream){
		func(s *Stream) { s.Name = "../logs" }, func(s *Stream) { s.Title = " " }, func(s *Stream) { s.Storage = StorageConfig{} },
		func(s *Stream) { s.Storage.RetentionDays = -1 }, func(s *Stream) { s.Storage.MaxAgeHours = 0; s.Storage.RetentionDays = 7 },
		func(s *Stream) { s.Routing.Enabled = true }, func(s *Stream) { s.Routing.Priority = 1001 },
		func(s *Stream) { s.Routing.Rules = []RoutingRule{{Field: "broken[", Operator: "equals", Value: "x"}} },
	} {
		stream := validStream("payments")
		change(&stream)
		if ValidateStream(stream) == nil {
			t.Fatalf("accepted invalid stream: %+v", stream)
		}
	}
}

func TestStreamSettingsPersistAndCannotOverwriteDuplicate(t *testing.T) {
	ctx, database := testDB(t)
	store := NewStreamStore(database)
	stream := validStream("payments")
	stream.Routing = RoutingConfig{Enabled: true, Priority: 5, Match: "any", Rules: []RoutingRule{{Field: "source", Operator: "equals", Value: "billing"}}}
	saved, err := store.Save(ctx, stream, true)
	if err != nil || saved.ID == "" {
		t.Fatalf("%+v %v", saved, err)
	}
	saved.Title = "Updated payments"
	if _, err := store.Save(ctx, saved, false); err != nil {
		t.Fatal(err)
	}
	listed, err := store.List(ctx)
	if err != nil || len(listed) != 1 || listed[0].Title != "Updated payments" || listed[0].Routing.Rules[0].Value != "billing" || listed[0].Storage.MaxDocuments != 2 {
		t.Fatal(listed, err)
	}
	if _, err := store.Save(ctx, stream, true); err != ErrStreamExists {
		t.Fatal(err)
	}
}
