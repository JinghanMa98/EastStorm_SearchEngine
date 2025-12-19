package cis5550.jobs;
import cis5550.jobs.Crawler;

public class TestNormalize {
    public static void main(String[] args) {
        test("https://foo.com:8000/bar/xyz.html", "http://elsewhere.com/abc.html");
        test("https://foo.com:8000/bar/xyz.html", "/one/two.html");
        test("https://foo.com:8000/bar/xyz.html", "../blubb/123.html");
        test("https://foo.com:8000/bar/xyz.html", "#frag");
        test("https://foo.com:8000/bar/xyz.html", "blah.html#test");
        test("https://foo.com:8000/bar/xyz.html", "foo/blah.html");
        test("https://foo.com:8000/bar/xyz.html", "#");
        test("https://foo.com:8000/bar/foo/xyz.html", "blubb/123.html");
        test("https://foo.com:8000/bar/foo/xyz.html", "../blubb/123.html");
        test("https://foo.com:8000/bar/foo/xyz.html", "../../blubb/123.html");
        test("https://foo.com:8000/bar/foo/xyz.html", "../../../blubb/123.html");
        test("https://foo.com:8000/bar/foo/xyz.html", "../../../../blubb/123.html");





    }

    static void test(String base, String raw) {
        try {
            String normalized = Crawler.normalizeURL(base, raw);
            System.out.println("BASE = " + base);
            System.out.println("RAW  = " + raw);
            System.out.println("OUT  = " + normalized);
            System.out.println("-----------------------------------");
        } catch (Exception e) {
            System.out.println("BASE = " + base);
            System.out.println("RAW  = " + raw);
            System.out.println("ERROR: " + e.getMessage());
            System.out.println("-----------------------------------");
        }
    }
}