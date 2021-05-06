package dbpath

func ToCanonical(pth string) (string, error) {
	parts, err := Split(pth)
	if err != nil {
		return "", err
	}

	return Join(parts...), nil

}
