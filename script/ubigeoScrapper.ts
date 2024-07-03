import puppeteer from "puppeteer";

//https://prod3.seace.gob.pe/portalseace-uiwd-pub/ControllerServletOpen?pageid=0&portletid=BuscadorUbigeo&scriptdo=doView&page__ind=&v_dep_cod=01&v_pro_cod=01&v_dis_cod=01

const trueSource = 'https://prod3.seace.gob.pe/portalseace-uiwd-pub/ControllerServletOpen?pageid=0&portletid=BuscadorUbigeo&scriptdo=doView&page__ind=';

const browser = await puppeteer.launch();
const page = await browser.newPage();
await page.goto(trueSource);

type District = {
	code: string;
	value: string;
};

type Province = {
	code: string;
	value: string;
	disctricts: District[];
};

type Region = {
	code: string;
	value: string;
	provinces: Province[];
};

const exteriorRegion: Region = {
	code: "98",
	value: "EXTERIOR",
	provinces: [
		{
			code: "01",
			value: "EXTERIOR",
			disctricts: [
				{
					code: "00",
					value: "MULTIDISTRITAL"
				}
			]
		}
	]
}

// Extract data
const regionsWithoutProvinces = await page.evaluate(() => {
	const $regionsSelector = document.querySelector(`select[name="v_dep_cod"]`);

	if (!$regionsSelector) {
		return null;
	}

	const $regionOptions = $regionsSelector.querySelectorAll("option");

	if (!$regionOptions) {
		return null;
	}

	const regionsWithoutProvinces = Array
		.from($regionOptions)
		.map($region => {
			const regionCode = $region.getAttribute("value")!;

			if (regionCode === "00" || regionCode === "98") {
				return null;
			}

			const regionValue = $region.textContent?.replace(`${regionCode}-`, "").trim()!;

			const region: Omit<Region, "provinces"> = {
				code: regionCode,
				value: regionValue
			}

			return region;
		})
		.filter(Boolean) as Omit<Region, "provinces">[];

	return regionsWithoutProvinces;
});

console.log(regionsWithoutProvinces);

await browser.close();