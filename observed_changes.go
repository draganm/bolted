package bolted

import "github.com/draganm/bolted/dbpath"

type ChangeType int

const (
	ChangeTypeNoChange ChangeType = iota
	ChangeTypeMapCreated
	ChangeTypeValueSet
	ChangeTypeDeleted
)

// type ObservedChanges map[string]ChangeType
type ObservedChange struct {
	Path dbpath.Path
	Type ChangeType
}

type ObservedChanges []ObservedChange

func (o ObservedChanges) TypeOfChange(path dbpath.Path) ChangeType {
	for _, oc := range o {
		switch oc.Type {
		case ChangeTypeDeleted:
			if oc.Path.ToMatcher().Matches(path) {
				return ChangeTypeDeleted
			}
		case ChangeTypeMapCreated, ChangeTypeValueSet:
			if path.Equal(oc.Path) {
				return oc.Type
			}
		}
	}
	return ChangeTypeNoChange
}

func (o ObservedChanges) Update(path dbpath.Path, t ChangeType) ObservedChanges {
	switch t {
	case ChangeTypeValueSet, ChangeTypeMapCreated:
		for i, oc := range o {
			if oc.Path.Equal(path) {
				o[i].Type = t
				return o
			}
		}
		return append(o, ObservedChange{Path: path, Type: t})
	case ChangeTypeDeleted:
		m := path.ToMatcher().AppendAnySubpathMatcher()
		oc := ObservedChanges{}
		for _, c := range o {
			if !m.Matches(c.Path) {
				oc = append(oc, c)
			}
		}

		oc = append(oc, ObservedChange{Path: path, Type: t})
		return oc
	default:
		return o
	}
}
