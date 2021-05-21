package dbpath_test

import (
	"testing"

	"github.com/draganm/bolted/dbpath"
	"github.com/stretchr/testify/require"
)

func TestMatcher(t *testing.T) {

	cases := []struct {
		name    string
		matcher dbpath.Matcher
		path    dbpath.Path
		matches bool
	}{
		{
			name:    "empty matcher matches empty path",
			matcher: dbpath.Matcher{},
			path:    dbpath.Path{},
			matches: true,
		},
		{
			name:    "empty matcher does not match not empty path",
			matcher: dbpath.Matcher{},
			path:    dbpath.ToPath("abc"),
			matches: false,
		},
		{
			name:    "one element matcher matches same element path",
			matcher: dbpath.ToPath("abc").ToMatcher(),
			path:    dbpath.ToPath("abc"),
			matches: true,
		},
		{
			name:    "one element matcher does not match different element path",
			matcher: dbpath.ToPath("abc").ToMatcher(),
			path:    dbpath.ToPath("def"),
			matches: false,
		},
		{
			name:    "one element matcher does not match empty path",
			matcher: dbpath.ToPath("abc").ToMatcher(),
			path:    dbpath.ToPath(),
			matches: false,
		},
		{
			name:    "one element matcher does not match two element path",
			matcher: dbpath.ToPath("abc").ToMatcher(),
			path:    dbpath.ToPath("abc", "def"),
			matches: false,
		},
		{
			name:    "two elements matcher matches two element path",
			matcher: dbpath.ToPath("abc").ToMatcher().AppendExactMatcher("def"),
			path:    dbpath.ToPath("abc", "def"),
			matches: true,
		},
		{
			name:    "single any element matcher does not match empty path",
			matcher: dbpath.Matcher{}.AppendAnyElementMatcher(),
			path:    dbpath.ToPath(),
			matches: false,
		},
		{
			name:    "single any element matcher matches any one element path",
			matcher: dbpath.Matcher{}.AppendAnyElementMatcher(),
			path:    dbpath.ToPath("abc"),
			matches: true,
		},
		{
			name:    "single any element does not match any two element paths",
			matcher: dbpath.Matcher{}.AppendAnyElementMatcher(),
			path:    dbpath.ToPath("abc", "def"),
			matches: false,
		},
		{
			name:    "any subpath matcher matches empty path",
			matcher: dbpath.Matcher{}.AppendAnySubpathMatcher(),
			path:    dbpath.ToPath(),
			matches: true,
		},
		{
			name:    "any subpath matcher matches single element path",
			matcher: dbpath.Matcher{}.AppendAnySubpathMatcher(),
			path:    dbpath.ToPath("abc"),
			matches: true,
		},
		{
			name:    "any subpath matcher matches two elements path",
			matcher: dbpath.Matcher{}.AppendAnySubpathMatcher(),
			path:    dbpath.ToPath("abc", "def"),
			matches: true,
		},
		{
			name:    "any subpath matcher following one element matcher matches one element path",
			matcher: dbpath.ToPath("abc").ToMatcher().AppendAnySubpathMatcher(),
			path:    dbpath.ToPath("abc"),
			matches: true,
		},
		{
			name:    "any subpath matcher following one element matcher matches two elements path",
			matcher: dbpath.ToPath("abc").ToMatcher().AppendAnySubpathMatcher(),
			path:    dbpath.ToPath("abc", "def"),
			matches: true,
		},
		{
			name:    "any subpath matcher following one element matcher matches three elements path",
			matcher: dbpath.ToPath("abc").ToMatcher().AppendAnySubpathMatcher(),
			path:    dbpath.ToPath("abc", "def", "ghi"),
			matches: true,
		},
		{
			name:    "any subpath matcher following one element matcher does not match if the prefix is wrong",
			matcher: dbpath.ToPath("abc").ToMatcher().AppendAnySubpathMatcher(),
			path:    dbpath.ToPath("def"),
			matches: false,
		},

		{
			name:    "any subpath matcher followed by one element matcher matches one element path",
			matcher: dbpath.ToPath().ToMatcher().AppendAnySubpathMatcher().AppendExactMatcher("abc"),
			path:    dbpath.ToPath("abc"),
			matches: true,
		},
		{
			name:    "any subpath matcher followed by one element matcher does not match empty path",
			matcher: dbpath.ToPath().ToMatcher().AppendAnySubpathMatcher().AppendExactMatcher("abc"),
			path:    dbpath.ToPath(),
			matches: false,
		},
		{
			name:    "any subpath matcher followed by one element matcher does match that one element",
			matcher: dbpath.ToPath().ToMatcher().AppendAnySubpathMatcher().AppendExactMatcher("abc"),
			path:    dbpath.ToPath("abc"),
			matches: true,
		},
		{
			name:    "any subpath matcher followed by one element matcher does match that one element prefixed by other element element",
			matcher: dbpath.ToPath().ToMatcher().AppendAnySubpathMatcher().AppendExactMatcher("abc"),
			path:    dbpath.ToPath("foo", "abc"),
			matches: true,
		},
		{
			name:    "any subpath matcher followed by one element matcher does match that one element prefixed by two other elements",
			matcher: dbpath.ToPath().ToMatcher().AppendAnySubpathMatcher().AppendExactMatcher("abc"),
			path:    dbpath.ToPath("foo", "bar", "abc"),
			matches: true,
		},
		{
			name:    "any subpath matcher followed by one element matcher does not match different element",
			matcher: dbpath.ToPath().ToMatcher().AppendAnySubpathMatcher().AppendExactMatcher("abc"),
			path:    dbpath.ToPath("foo"),
			matches: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			matches := c.matcher.Matches(c.path)
			require.Equal(t, c.matches, matches)
		})
	}
}
