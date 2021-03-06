"""
This file will contains the logic for phrase match
"""
import spacy
from spacy.matcher import PhraseMatcher


class Matcher:
    def __init__(self):
        print("INSIDE Suggection_product file")
        self.nlp = spacy.load('en_core_web_sm')
        self.matcher = PhraseMatcher(self.nlp.vocab)
        with open("phrase.txt", encoding="unicode_escape") as f:
            self.data = self.nlp(f.read())
        print(self.data)

    def get_suggestion(self):
        suggestion_list = []
        phrase_list = ["Electric Blue"]
        phrase_pattern = [self.nlp.make_doc(text) for text in phrase_list]
        self.matcher.add("Phone", None, *phrase_pattern)
        found_matches = self.matcher(self.data)

        # for sent in self.data.sents:
        #     for match_id, start, end in self.matcher(self.nlp(sent.text)):
        #         if self.nlp.vocab.strings[match_id] in ["Phone"]:
        #             print(sent.text)
        #
        # quit()
        for match_id, start, end in found_matches:
            string_id = self.nlp.vocab.strings[match_id]
            span = self.data[start:end]
            suggestion_list.append(span.text)
        return suggestion_list

