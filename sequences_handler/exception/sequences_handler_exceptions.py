class SequencesHandlerError(Exception):
    pass


class InvalidSequenceLineFormatLengthError(SequencesHandlerError):
    pass


class InvalidSequencesLineCountError(SequencesHandlerError):
    pass
