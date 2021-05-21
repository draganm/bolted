package dbpath

import (
	"net/url"
	"strings"

	"github.com/pkg/errors"
)

type Path []string

func (p Path) Append(elements ...string) Path {
	cp := make(Path, len(p)+len(elements))
	copy(cp, p)
	for i, e := range elements {
		cp[len(p)+i] = e
	}
	return cp
}

func (p Path) String() string {
	return Join(p...)
}

func (p Path) ToMatcher() Matcher {
	m := make(Matcher, len(p))
	for i, pe := range p {
		m[i] = exactMatcher(pe)
	}
	return m
}

func (p Path) IsPrefixOf(o Path) bool {
	if len(p) > len(o) {
		return false
	}
	for i, e := range p {
		if e != o[i] {
			return false
		}
	}
	return true
}

func Parse(p string) (Path, error) {
	parts, err := Split(p)
	if err != nil {
		return nil, err
	}
	return Path(parts), nil
}

func ToPath(p ...string) Path {
	return Path(p)
}

var NilPath = Path(nil)

const Separator = "/"

func Split(path string) ([]string, error) {

	parts := strings.Split(path, Separator)

	res := []string{}

	for i, p := range parts {
		up, err := UnescapePart(p)
		if err != nil {
			return nil, errors.Wrapf(err, "while unescaping part at position %d: %q", i, p)
		}
		if up != "" {
			res = append(res, up)
		}
	}

	return res, nil
}

func Join(parts ...string) string {
	escaped := make([]string, len(parts))
	for i, p := range parts {
		escaped[i] = EscapePart(p)
	}
	return strings.Join(escaped, Separator)
}

func Append(pth string, parts ...string) string {
	if pth == "" {
		return Join(parts...)
	}

	escaped := make([]string, len(parts)+1)
	escaped[0] = pth
	for i, p := range parts {
		escaped[i+1] = EscapePart(p)
	}
	return strings.Join(escaped, Separator)
}

func EscapePart(part string) string {
	return url.PathEscape(part)
}

func UnescapePart(part string) (string, error) {
	return url.PathUnescape(part)
}
