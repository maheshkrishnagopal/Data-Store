class SizeError(Exception):
    """

    """
    def __init__(self, msg):
        """

        """
        print(msg)


class LengthError(Exception):
    """

    """
    def __init__(self, error_msg):
        """

        :param error_msg:
        """
        print(error_msg)


class KeyExistsError(Exception):
    """

    """
    def __init__(self, err_msg):
        """

        :param err_msg:
        """
        print(err_msg)


class KeyNotExistError(Exception):
    """

    """
    def __init__(self, err_msg):
        """

        :param err_msg:
        """
        print(err_msg)


class TTLOverDueError(Exception):
    """

    """
    def __init__(self, msg):
        """

        :param msg:
        """
        print(msg)
