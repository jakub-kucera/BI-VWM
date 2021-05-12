import pytest

from rtree.data.mbb import MBB, MBBDim


@pytest.mark.parametrize('mbb_dim, low, high, size', [
    ((MBBDim(5, 10),), 5, 10, 5),
    ((MBBDim(10, 5),), 5, 10, 5),
    ((MBBDim(5, 5),), 5, 5, 0)
])
def test_mbb_basic(mbb_dim, low, high, size):
    mbb = MBB(mbb_dim)

    assert mbb.box[0].low == low
    assert mbb.box[0].high == high
    assert mbb.size == size


@pytest.mark.parametrize('mbb_dim1, mbb_dim2', [
    (MBBDim(5, 10), MBBDim(11, 25)),
    (MBBDim(1, 20), MBBDim(6, 80)),
    (MBBDim(1, 1), MBBDim(6, 80)),
    (MBBDim(1, 20), MBBDim(6, 6)),
    (MBBDim(1, 1), MBBDim(6, 6)),
])
def test_mbb_size(mbb_dim1, mbb_dim2):
    mbb_dim1 = MBBDim(5, 10)
    mbb_dim2 = MBBDim(11, 25)

    input_dims = (mbb_dim1, mbb_dim2)
    assert MBB.get_size(input_dims) == (mbb_dim1.low - mbb_dim1.high) * (mbb_dim2.low - mbb_dim2.high)


@pytest.mark.parametrize('mbb1, mbb2', [
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), MBB((MBBDim(5, 10), MBBDim(11, 25)))),
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), MBB((MBBDim(1, 5), MBBDim(11, 25)))),
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), MBB((MBBDim(5, 10), MBBDim(25, 36)))),
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), MBB((MBBDim(1, 10), MBBDim(25, 36)))),
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), MBB((MBBDim(1, 5), MBBDim(25, 36))))
])
def test_mbb_overlap(mbb1, mbb2):
    assert mbb1.overlaps(mbb2)
    assert mbb1.overlaps(mbb2) == mbb2.overlaps(mbb1)


@pytest.mark.parametrize('mbb1, mbb2', [
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), MBB((MBBDim(5, 10), MBBDim(26, 36)))),
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), MBB((MBBDim(11, 40), MBBDim(11, 25)))),
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), MBB((MBBDim(11, 40), MBBDim(26, 36)))),
])
def test_mbb_not_overlap(mbb1, mbb2):
    assert not mbb1.overlaps(mbb2)
    assert mbb1.overlaps(mbb2) == mbb2.overlaps(mbb1)


@pytest.mark.parametrize('mbb_old, new_box, diff', [
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), (MBBDim(5, 10), MBBDim(11, 25)), 0),
    (MBB((MBBDim(5, 10), MBBDim(1, 1))), (MBBDim(5, 100), MBBDim(1, 1)), 0),
    (MBB((MBBDim(20, 20), MBBDim(11, 100000))), (MBBDim(20, 20), MBBDim(11, 25)), 0),
    (MBB((MBBDim(0, 10), MBBDim(0, 0))), (MBBDim(0, 10), MBBDim(0, 10)), 100),
    (MBB((MBBDim(0, 10), MBBDim(0, 10))), (MBBDim(0, 10), MBBDim(0, 0)), 0),
    (MBB((MBBDim(0, 10), MBBDim(0, 10))), (MBBDim(0, 10), MBBDim(0, -10)), 100),
    (MBB((MBBDim(0, 10), MBBDim(0, 0))), (MBBDim(0, 10), MBBDim(-10, 10)), 200),
])
def test_mbb_size_increase_insert(mbb_old, new_box, diff):
    assert mbb_old.size_increase_insert(new_box) == diff
    # assert mbb1.overlaps(mbb2) == mbb2.overlaps(mbb1)


@pytest.mark.parametrize('mbb_old, box_new, mbb_new', [
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), (MBBDim(5, 10), MBBDim(11, 25)), MBB((MBBDim(5, 10), MBBDim(11, 25)))),
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), (MBBDim(5, 10), MBBDim(10, 25)), MBB((MBBDim(5, 10), MBBDim(10, 25)))),
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), (MBBDim(5, 10), MBBDim(10, 50)), MBB((MBBDim(5, 10), MBBDim(10, 50)))),
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), (MBBDim(-100, 200), MBBDim(11, 25)),
     MBB((MBBDim(-100, 200), MBBDim(11, 25)))),
    (MBB((MBBDim(5, 10), MBBDim(11, 25))), (MBBDim(-100, 200), MBBDim(2, 50)), MBB((MBBDim(-100, 200), MBBDim(2, 50)))),
])
def test_mbb_insert(mbb_old, box_new, mbb_new):
    mbb_old.insert_mbb(new_mbb=box_new)
    assert mbb_old == mbb_new
    mbb_new.insert_mbb(new_mbb=box_new)
    assert mbb_old == mbb_new
