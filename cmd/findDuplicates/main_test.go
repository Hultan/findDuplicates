package main

import (
	"reflect"
	"testing"
)

func Test_scanDirectoryForFileNames(t *testing.T) {
	type args struct {
		dir     string
		pattern string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{"empty folder", args{emptyFolder, ".go$"}, []string{}},
		{"test folder", args{testFolder, ".go$"}, []string{"/home/per/code/findDuplicates/test/walk/walk.go"}},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := scanDirectoryForFileNames(tt.args.dir, tt.args.pattern)
				if err != nil {
					t.Errorf("scanDirectoryForFileNames() returned an error : %v", err)
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("scanDirectoryForFileNames() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}