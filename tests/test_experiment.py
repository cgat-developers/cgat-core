import cgatcore.experiment as E


def test_start_and_stop_are_logged_with_optparse():
    statement = (
        f"python -c 'import cgatcore.experiment as E; options, args = E.start(parser=E.OptionParser()); E.stop()'")

    stdout = E.run(statement, return_stdout=True)
    assert "job started" in stdout
    assert "job finished" in stdout


def test_start_and_stop_are_logged_with_argparse():
    statement = (
        f"python -c 'import cgatcore.experiment as E; options = E.start(parser=E.ArgumentParser()); E.stop()'")

    stdout = E.run(statement, return_stdout=True)
    assert "job started" in stdout
    assert "job finished" in stdout
