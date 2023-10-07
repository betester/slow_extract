package stemmer

import "github.com/RadhiFadlillah/go-sastrawi"

type Stemmer interface {
	StemWord(string) string
	StemSentence(string) []string
}

type SastrawiStemmer struct {
	Dictionary sastrawi.Dictionary
}

func (ss *SastrawiStemmer) StemWord(word string) string {
	stemmer := sastrawi.NewStemmer(ss.Dictionary)

	return stemmer.Stem(word)
}

func (ss *SastrawiStemmer) StemSentence(sentence string) []string {
	stemmer := sastrawi.NewStemmer(ss.Dictionary)
	stemmedSentence := make([]string, 0)
	for _, word := range sastrawi.Tokenize(sentence) {
		stemmedSentence = append(stemmedSentence, stemmer.Stem(word))
	}

	return stemmedSentence
}

