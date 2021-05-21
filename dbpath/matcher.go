package dbpath

type Matcher []matcherElement

func (m Matcher) Matches(p Path) bool {
	if len(m) == 0 && len(p) == 0 {
		return true
	}

	if len(m) == 0 {
		return false
	}

	return m[0].matches(p, m[1:])
}

type matcherElement interface {
	matches(p Path, rhs Matcher) bool
}

type exactMatcher string

func (e exactMatcher) matches(p Path, rhs Matcher) bool {
	if len(p) == 0 {
		return false
	}

	if string(p[0]) != string(e) {
		return false
	}

	return rhs.Matches(p[1:])
}

type anyElementMatcher struct{}

func (a anyElementMatcher) matches(p Path, rhs Matcher) bool {
	if len(p) == 0 {
		return false
	}

	return rhs.Matches(p[1:])
}

type anySubpathMatcher struct{}

func (a anySubpathMatcher) matches(p Path, rhs Matcher) bool {

	if len(rhs) == 0 {
		return true
	}

	for i := range p {
		if rhs.Matches(p[i:]) {
			return true
		}
	}
	return false

	// if len(p) == 1 {
	// 	return true
	// }

	// return false
}

func (m Matcher) AppendExactMatcher(e string) Matcher {
	cp := make(Matcher, len(m)+1)
	copy(cp, m)
	cp[len(m)] = exactMatcher(e)
	return cp
}

func (m Matcher) AppendAnyElementMatcher() Matcher {
	cp := make(Matcher, len(m)+1)
	copy(cp, m)
	cp[len(m)] = anyElementMatcher{}
	return cp
}

func (m Matcher) AppendAnySubpathMatcher() Matcher {
	cp := make(Matcher, len(m)+1)
	copy(cp, m)
	cp[len(m)] = anySubpathMatcher{}
	return cp
}
