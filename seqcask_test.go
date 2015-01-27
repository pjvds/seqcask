package seqcask_test

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/pjvds/seqcask"
	"github.com/stretchr/testify/assert"
)

func RandomValue(length int) []byte {
	value := make([]byte, length, length)
	for i := 0; i < length; i++ {
		value[i] = byte(rand.Intn(255))
	}

	return value
}

func BenchmarkPut(b *testing.B) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	random := seqcask.NewRandomValueGenerator(200)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cask.Put(<-random.Values); err != nil {
			b.Fatalf("failed to put: %v", err.Error())
		}
	}
	cask.Sync()
}

func TestPutBatchGetAll(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	batch := seqcask.NewWriteBatch()

	putValues := make([][]byte, 50, 50)
	for index := range putValues {
		value := RandomValue(200)

		putValues[index] = value
		batch.Put(value)
	}

	err := cask.Write(batch)
	assert.Nil(t, err)

	values, err := cask.GetAll(uint64(0), len(putValues))

	assert.Equal(t, len(values), len(putValues))

	for index, value := range values {
		assert.Equal(t, value.Value, putValues[index])
	}
}

func BenchmarkPutBatch(b *testing.B) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	batch := seqcask.NewWriteBatch()
	batch.Put(
		[]byte("agxtzwepimobpnikebkhftxcfslqtnnsgihzdcuvgtlptjxjxrblxnonvazjeqiahfxjszxcpxoqlxudrsndeuodeqaeiotrczusvftchpxnxfmkejvqqpvrfcvpcuafplfwpimrmklftbrdjmfaxapnqpcvcsmgnisczvjnypmyffexxuzovbwzrjghjtziudfgbqbrhazcdcyzkxqjbxnoscpuvzaoawwclllfbwmkhhqxcnavfwfglmmaamf"),
		[]byte("smlqfbwpqsethbgweampbwfvcuktntarrowoicnooqoedmlcjyhyirfcwzejgafzgeqhjgocgredqotjivpludcfbusccknredakjjfzimlamxhddxiiqqwctzrmsfoymwkjuwagmghesefpyjrxsagwbpyuvwcnjhusfwuvalxmstctvhljuocrefeehccwdhjmwlyluycjsuzkwcaywerjdzywatuxnpuluaixskgmtxqmkrwwwffhiysnzzy"),
		[]byte("mtuaizgmqjqsvasmapkdxnygadhwrzusaiffteljlrmffwtzooljihllqpgeeokbawevvidxdtwnynjtrlxbztlaotfvvulqwdashfaviitwzzccqdvausmqzkmovtuisjnjdwqbsyfdilkgpriddxgsqmveenqihxiwkzpuasxbfhfajazirfwhntrwsqmpxbxoosqlvspomqbifcofbnhmdjpxfhuvorbhfrzlvyzpmacigypavxtnjbtbdgh"),
		[]byte("xuonnyzlxxpzsbrmlvtfwdswmsotusqtprdomlfpxfwwwvozusdgzbsflqwtqibczwiksefczjjzcorufmxujxjqvematcfeofmgqigklnkqmsjihansqixxcjabsnutrusscmofkujfmufbgbqxlbgzuwnpwfvzikigyhfhnbswdsiukjlsztjtqkyoertlvylptacsqapmjermfsfpkqsvvabtfleygskogwvflwpjfzgvvuyoupiqlvvphhs"),
		[]byte("bmgmbcwrmsmgwnkeyhahnitexivcvxblaptgcaqcozpjyetpmgnmndzvlysmxfijogjytcczwohqijcojdhotfnavijupuaruyywztaqqcmmnbxrfwszcfzyrzieoudhjsxfrulzbvpcbmpzuifciejnyyqbfwviyzlyhmvkcgpqcjdacrfszhtmzejdyqfjuiojovoiyuwbfsaxbteifqmciznabveigqlkwczwshdhzenjlmhgdclhkgtmmaj"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cask.Write(batch); err != nil {
			b.Fatalf("failed to write batch: %v", err.Error())
		}
	}
	cask.Sync()
}

func TestCreate(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 0)
	defer cask.Close()
}

func TestPut(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	defer cask.Close()

	putValues := [][]byte{
		[]byte("pieter joost van de sande"),
		[]byte("tomas roos"),
	}

	for _, value := range putValues {
		if err := cask.Put(value); err != nil {
			t.Fatalf("failed to put: %v", err.Error())
		}
	}
}

func TestPutGetRoundtrup(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	defer cask.Close()

	putValue := []byte("hello world")

	if err := cask.Put(putValue); err != nil {
		t.Fatalf("failed to put: %v", err.Error())
	} else {
		if err := cask.Sync(); err != nil {
			t.Fatalf("failed to sync: %v")
		}

		if getValue, err := cask.Get(0); err != nil {
			t.Fatalf("failed to get: %v", err.Error())
		} else {
			if !bytes.Equal(putValue, getValue.Value) {
				t.Fatalf("put and get value differ: %v vs %v, %v vs %v", string(putValue), string(getValue.Value), putValue, getValue.Value)
			}
		}
	}
}
