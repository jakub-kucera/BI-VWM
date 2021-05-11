from rtree.data.mbb import MBBDim


def test_mbbdim_1():
    mbb_dim = MBBDim(5, 10)
    assert mbb_dim.low == 5
    assert mbb_dim.high == 10


def test_mbbdim_2():
    mbb_dim = MBBDim(5, 10)
    assert mbb_dim.low == 5
    assert mbb_dim.high == 10


def test_mbbdim_diff():
    mbb_dim = MBBDim(5, 10)
    assert mbb_dim.get_diff() == 5


def test_mbbdim_overlaps_1():
    mbb_dim1 = MBBDim(5, 10)
    mbb_dim2 = MBBDim(5, 10)
    assert mbb_dim1.overlaps(mbb_dim2) is True
    assert mbb_dim1.overlaps(mbb_dim2) == mbb_dim2.overlaps(mbb_dim1)


def test_mbbdim_overlaps_2():
    mbb_dim1 = MBBDim(4, 10)
    mbb_dim2 = MBBDim(5, 10)
    assert mbb_dim1.overlaps(mbb_dim2) is True
    assert mbb_dim1.overlaps(mbb_dim2) == mbb_dim2.overlaps(mbb_dim1)


def test_mbbdim_overlaps_3():
    mbb_dim1 = MBBDim(5, 10)
    mbb_dim2 = MBBDim(2, 20)
    assert mbb_dim1.overlaps(mbb_dim2) is True
    assert mbb_dim1.overlaps(mbb_dim2) == mbb_dim2.overlaps(mbb_dim1)


def test_mbbdim_overlaps_4():
    mbb_dim1 = MBBDim(1, 5)
    mbb_dim2 = MBBDim(5, 10)
    assert mbb_dim1.overlaps(mbb_dim2) is True
    assert mbb_dim1.overlaps(mbb_dim2) == mbb_dim2.overlaps(mbb_dim1)


def test_mbbdim_overlaps_5():
    mbb_dim1 = MBBDim(1, 5)
    mbb_dim2 = MBBDim(6, 10)
    assert mbb_dim1.overlaps(mbb_dim2) is False
    assert mbb_dim1.overlaps(mbb_dim2) == mbb_dim2.overlaps(mbb_dim1)


def test_mbbdim_contains_1():
    mbb_dim1 = MBBDim(5, 10)
    mbb_dim2 = MBBDim(5, 10)
    assert mbb_dim1.contains(mbb_dim2) is True
    assert mbb_dim1.contains(mbb_dim2) == mbb_dim2.contains(mbb_dim1)


def test_mbbdim_contains_2():
    mbb_dim1 = MBBDim(4, 10)
    mbb_dim2 = MBBDim(5, 10)
    assert mbb_dim1.contains(mbb_dim2) is True
    assert mbb_dim1.contains(mbb_dim2) != mbb_dim2.contains(mbb_dim1)


def test_mbbdim_contains_3():
    mbb_dim1 = MBBDim(5, 10)
    mbb_dim2 = MBBDim(3, 20)
    assert mbb_dim1.contains(mbb_dim2) is False
    assert mbb_dim1.contains(mbb_dim2) != mbb_dim2.contains(mbb_dim1)


def test_mbbdim_contains_5():
    mbb_dim1 = MBBDim(5, 10)
    mbb_dim2 = MBBDim(10, 20)
    assert mbb_dim1.contains(mbb_dim2) is False
    assert mbb_dim1.contains(mbb_dim2) == mbb_dim2.contains(mbb_dim1)
