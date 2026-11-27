// Package convert преобразует коллекции между публичными и внутренними типами.
package convert

// Slice сохраняет порядок элементов; пустой результат можно сразу сериализовать как [].
func Slice[A, B any](values []A, convert func(A) B) []B {
	result := make([]B, len(values))
	for i, value := range values {
		result[i] = convert(value)
	}

	return result
}

// First возвращает первую необязательную настройку или нулевое значение.
func First[T any](values []T) (value T) {
	if len(values) > 0 {
		return values[0]
	}

	return value
}
